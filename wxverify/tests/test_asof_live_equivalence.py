"""§18.6 live-equivalence oracles for the as-of machinery (plan §6).

Two invariants of the as-of parameterization:

* ``as_of=None`` composes SQL byte-identical to production — the live path
  never carries a knowability or availability clause (the bit-identity
  lever: any mutation that forks the SQL shape trips this family);
* at T = now, the as-of path and the live production path agree
  field-for-field on fully-knowable data, diverging ONLY where
  ``first_known_at`` IS NULL (multimodel virtual pairs by design, pre-v4
  backfilled rows) — a documented, pinned divergence, not a defect.

The as-of path is a declared-configuration counterfactual: ``min_n`` and the
window come from the run's pinned snapshot, never live mutable settings.
All fixture values are synthetic.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from tests.helpers import (
    asof_conn,
    asof_insert_pair,
    asof_insert_sample,
    asof_make_real_feed,
    asof_make_site,
    asof_persistence_feed_id,
)
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.forecast.data import forecast_ranking, load_future_samples
from wxverify.scoring.leaderboard import LeaderboardRow, asof_leaderboard, leaderboard
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import set_setting
from wxverify.verification.asof import pair_knowability_exclusions


def _scored_fields(row: LeaderboardRow) -> tuple[object, ...]:
    """Every field except window_key/window_days, which label the path
    ('live:30d' vs 'asof:30d') and differ by design."""
    return (
        row.feed_id,
        row.source,
        row.model,
        row.n,
        row.skill_score,
        row.badge,
        row.below_baseline,
        row.confident,
        row.bias,
        row.mae,
        row.rmse,
    )


def test_live_path_sql_never_carries_knowability_or_availability_clause() -> None:
    """Bit-identity lever (§18.6): with ``as_of=None`` every statement the
    live read path executes is the production statement — no
    ``first_known_at`` term, no observation knowability probe, no
    ``fetched_at`` availability bound. A mutation that applies the as-of
    clause unconditionally (e.g. defaulting T to "now" or a sentinel) forks
    the production SQL shape and fails here.

    Kills: `_asof_clause` (metrics.py) rewritten to substitute a sentinel T
    when ``as_of is None``; the data.py as-of availability clause applied
    unconditionally.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "SQL Shape Site")
    feed_id = asof_make_real_feed(conn, "model-shape")
    persistence = asof_persistence_feed_id(conn)
    now = utc_now()
    valid_at = isoformat_utc(now - timedelta(hours=6))
    issued_at = isoformat_utc(now - timedelta(hours=12))
    known_at = isoformat_utc(now - timedelta(hours=2))
    for fid, forecast in ((feed_id, 5.0), (persistence, 6.0)):
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=fid,
            valid_at=valid_at,
            issued_at=issued_at,
            forecast=forecast,
            observed=4.0,
            first_known_at=known_at,
        )
    # Sample stamps must match the strict `...Z` whole-second validation
    # LIKE pattern, so floor the wall clock before formatting.
    whole = now.replace(microsecond=0)
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        issued_at=isoformat_utc(whole - timedelta(hours=12)),
        valid_at=isoformat_utc(whole + timedelta(hours=6)),
        lead_hours=18,
        value=5.0,
        fetched_at=isoformat_utc(whole - timedelta(hours=11)),
    )
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        live_rows = leaderboard(
            conn,
            site_id=site_id,
            variable="temperature",
            day_ahead=1,
            window="30d",
        )
        strategy_for("temperature").aggregate(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=1,
            window_cutoff=None,
            min_n=1,
        )
        samples = load_future_samples(
            conn, site_id=site_id, since_valid_at=isoformat_utc(now)
        )
        forecast_ranking(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="30d"
        )
    finally:
        conn.set_trace_callback(None)
    # Non-vacuity: the traced calls really exercised the pair and sample
    # read paths and returned data.
    assert live_rows and samples
    assert any("FROM forecast_pairs" in s for s in statements)
    assert any("forecast_samples" in s for s in statements)
    offenders = [
        s
        for s in statements
        if "first_known_at" in s
        or "FROM observations ko" in s
        or "fetched_at IS NOT NULL" in s
    ]
    assert offenders == [], (
        "live-path (as_of=None) SQL must be byte-identical to production; "
        f"found as-of clause fragments in: {offenders}"
    )


def test_asof_at_now_equals_live_ranking_field_for_field() -> None:
    """§18.6 core: at T = now, on fully-knowable post-migration data, the
    as-of parameterized path and the live production path agree on every
    scored field — selection set, n, skill, badge, confidence, bias, mae,
    rmse — across multiple feeds, with the declared window anchored at T.

    An out-of-window OLD knowable pair sits 40 days back: both paths must
    exclude it under their 30-day windows.

    Kills: `asof_leaderboard` ignoring `window_days` (cutoff=None always) —
    the old pair would inflate the as-of n while the live path still
    excludes it.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Equivalence Matrix Site")
    alpha = asof_make_real_feed(conn, "model-eq-alpha")
    beta = asof_make_real_feed(conn, "model-eq-beta")
    persistence = asof_persistence_feed_id(conn)
    now = utc_now()
    known_at = isoformat_utc(now - timedelta(hours=2))
    issued_at = isoformat_utc(now - timedelta(hours=12))
    # Identical per-feed sq_errors keep every SQL aggregate order-independent,
    # so cross-path equality is exact, not approximate.
    for hours_ago in (6, 5, 4):
        valid_at = isoformat_utc(now - timedelta(hours=hours_ago))
        for fid, forecast in ((alpha, 5.0), (beta, 7.0), (persistence, 6.0)):
            asof_insert_pair(
                conn,
                site_id=site_id,
                feed_id=fid,
                valid_at=valid_at,
                issued_at=issued_at,
                forecast=forecast,
                observed=4.0,
                first_known_at=known_at,
            )
    # Knowable but 40 days old: outside the 30-day window on BOTH paths.
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=alpha,
        valid_at=isoformat_utc(now - timedelta(days=40)),
        issued_at=isoformat_utc(now - timedelta(days=40, hours=6)),
        forecast=4.0,
        observed=4.0,
        first_known_at=isoformat_utc(now - timedelta(days=40) + timedelta(hours=2)),
    )
    set_setting(conn, "min_n", "3")
    live = forecast_ranking(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="30d"
    )
    asof = forecast_ranking(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=isoformat_utc(now),
        declared_min_n=3,
        declared_window_days=30,
    )
    assert set(live) == set(asof) == {alpha, beta}
    for fid in (alpha, beta):
        assert _scored_fields(asof[fid]) == _scored_fields(live[fid])
    # Anchor the comparison in absolute terms so both paths agreeing on a
    # wrong value cannot slip through: alpha skill = 1 - 1/4, n = 3 (the
    # 40-day-old pair is windowed out), confident under min_n = 3.
    assert live[alpha].n == 3
    assert live[alpha].skill_score is not None
    assert abs(live[alpha].skill_score - 0.75) < 1e-12
    assert live[alpha].confident and asof[alpha].confident


def test_designed_divergence_null_first_known_is_live_only() -> None:
    """The §18.6 documented caveat, pinned as intended behavior: at T = now,
    a pair with NULL ``first_known_at`` (pre-v4 backfill / virtual-pair
    shape) participates in the LIVE aggregate but is excluded from the as-of
    aggregate with the recorded ``null_first_known_at`` reason. As-of and
    live diverge here BY DESIGN — and only here on this fixture.

    Kills: the ``first_known_at`` terms dropped from
    ``knowable_pair_predicate`` (the NULL row would become as-of visible,
    n 1 -> 2, and the exclusion count would drop to 0).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Null Divergence Site")
    feed_id = asof_make_real_feed(conn, "model-null-fka")
    now = utc_now()
    issued_at = isoformat_utc(now - timedelta(hours=12))
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=isoformat_utc(now - timedelta(hours=6)),
        issued_at=issued_at,
        forecast=5.0,
        observed=4.0,
        first_known_at=isoformat_utc(now - timedelta(hours=2)),
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=isoformat_utc(now - timedelta(hours=5)),
        issued_at=issued_at,
        forecast=9.0,
        observed=4.0,
        first_known_at=None,
    )
    t_now = isoformat_utc(now)
    live = strategy_for("temperature").aggregate(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_cutoff=None,
        min_n=1,
    )
    asof = strategy_for("temperature").aggregate(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_cutoff=None,
        min_n=1,
        as_of=t_now,
    )
    assert live.n == 2
    assert asof.n == 1
    assert asof.mae is not None and abs(asof.mae - 1.0) < 1e-12
    exclusions = pair_knowability_exclusions(
        conn, site_id=site_id, variable="temperature", day_ahead=1, as_of=t_now
    )
    assert exclusions.null_first_known_at == 1
    assert exclusions.total == 1


def test_asof_uses_declared_config_never_live_settings() -> None:
    """Declared-configuration counterfactual (§6/§18.6): the as-of path's
    confidence gate runs on the DECLARED ``min_n``, not the live mutable
    setting — and requesting an as-of ranking without a declared ``min_n``
    is a hard error, never a silent settings fallback.

    Live setting min_n = 1 (feed confident live at n = 2); declared
    min_n = 5 must yield confident False; declared min_n = 2 the paired
    positive True.

    Kills: `min_n_override` ignored in `_live_leaderboard` (unconditional
    `get_number_setting` read) — the declared-5 case would turn confident.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Declared Config Site")
    feed_id = asof_make_real_feed(conn, "model-declared")
    persistence = asof_persistence_feed_id(conn)
    known = "2035-01-02T07:00:00Z"
    for valid_at in ("2035-01-02T05:00:00Z", "2035-01-02T06:00:00Z"):
        for fid, forecast in ((feed_id, 5.0), (persistence, 6.0)):
            asof_insert_pair(
                conn,
                site_id=site_id,
                feed_id=fid,
                valid_at=valid_at,
                issued_at="2035-01-01T23:00:00Z",
                forecast=forecast,
                observed=4.0,
                first_known_at=known,
            )
    set_setting(conn, "min_n", "1")
    t = "2035-01-02T12:00:00Z"

    def asof_row(min_n: int) -> LeaderboardRow:
        rows = asof_leaderboard(
            conn,
            site_id=site_id,
            variable="temperature",
            day_ahead=1,
            as_of=t,
            min_n=min_n,
            window_days=None,
        )
        return {row.feed_id: row for row in rows}[feed_id]

    # Live control: the live path DOES read the setting (confident at n=2).
    live = {
        row.feed_id: row
        for row in leaderboard(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="30d"
        )
    }
    assert live[feed_id].n == 2 and live[feed_id].confident
    strict = asof_row(min_n=5)
    assert strict.n == 2
    assert strict.skill_score is not None  # gate fails on n, not on skill
    assert not strict.confident
    assert asof_row(min_n=2).confident  # paired positive
    with pytest.raises(ValueError, match="declared_min_n"):
        forecast_ranking(
            conn,
            site_id=site_id,
            variable="temperature",
            day_ahead=1,
            as_of=t,
        )
