"""Shared feed visibility predicates for scoring reads and materialization."""

from __future__ import annotations


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
