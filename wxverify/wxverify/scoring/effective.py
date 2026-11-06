"""Shared feed visibility predicates for scoring reads and materialization."""

from __future__ import annotations

# The largest day ahead of the canonical (variable, day ahead) grid.
MAX_DAY_AHEAD = 7


def active_feed_cte(*, site_param: str = "?") -> str:
    """CTE naming every feed that is a competitor at the given site.

    Hoists ``active_competitor_clause`` out of the per-pair-row scan: the
    predicate is a function of (feeds, site_feed_state) only and never of the
    pair row, so evaluating it once per feed is equivalent and turns its
    correlated EXISTS from ~200k evaluations into 31.

    NOTE: this CTE consumes TWO bind parameters when ``site_param`` is ``"?"`` —
    one for the LEFT JOIN and one inside ``active_competitor_clause`` itself,
    which carries its own ``{site_expr}`` placeholder for the meteoblue
    package-subscription EXISTS.
    """
    return f"""
        active_feeds AS (
            SELECT f.id AS feed_id
            FROM feeds f
            LEFT JOIN site_feed_state sfs
              ON sfs.site_id = {site_param} AND sfs.feed_id = f.id
            WHERE {active_competitor_clause(site_expr=site_param)}
        )"""


def active_competitor_clause(
    *,
    site_expr: str,
    feed_alias: str = "f",
    state_alias: str = "sfs",
) -> str:
    """SQL predicate for feeds that are competitors at a site.

    Meteoblue member feeds are scoring units, but their subscription is resolved
    through the site state of the `(meteoblue, multimodel)` package feed.
    """

    return f"""
    (
        {feed_alias}.is_virtual = 1
        OR (
            {feed_alias}.source = 'meteoblue'
            AND {feed_alias}.model != 'multimodel'
            AND {feed_alias}.enabled = 1
            AND EXISTS (
                SELECT 1
                FROM feeds pkg
                LEFT JOIN site_feed_state pkg_sfs
                  ON pkg_sfs.site_id = {site_expr}
                 AND pkg_sfs.feed_id = pkg.id
                WHERE pkg.source = 'meteoblue'
                  AND pkg.model = 'multimodel'
                  AND pkg.enabled = 1
                  AND COALESCE(pkg_sfs.enabled, pkg.default_subscribed) = 1
            )
        )
        OR (
            NOT (
                {feed_alias}.source = 'meteoblue'
                AND {feed_alias}.model != 'multimodel'
            )
            AND {feed_alias}.enabled = 1
            AND COALESCE({state_alias}.enabled, {feed_alias}.default_subscribed) = 1
        )
    )
    """
