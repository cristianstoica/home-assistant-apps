"""Shared test helpers for wxverify's pytest suite.

Kept deliberately small: only helpers actually reused across multiple test
modules belong here. Anything used by a single file stays local to it.
"""

from __future__ import annotations

from html.parser import HTMLParser

from wxverify.db.connection import _READ_POOL_SIZE, Database  # noqa: SLF001


def assert_read_pool_at_rest(db: Database) -> None:
    """Assert the read pool is back to its steady state: exactly
    ``_READ_POOL_SIZE`` connections queued, all of them distinct objects.

    Only valid once every dispatched read or drain has actually settled --
    calling this while a connection is still checked out (a read in flight,
    or a drain mid-cancellation-recovery) fails even against a correct
    implementation, since the pool is legitimately short during that window.
    A new recovery branch that forgets to publish a connection back (or
    publishes the same one twice) has nowhere to hide from this check.
    """
    pool = db._read_pool  # noqa: SLF001
    assert pool.qsize() == _READ_POOL_SIZE, (
        f"expected {_READ_POOL_SIZE} connections at rest, found {pool.qsize()}"
    )
    conns = list(pool._queue)  # noqa: SLF001
    ids = {id(conn) for conn in conns}
    assert len(ids) == len(conns) == _READ_POOL_SIZE, (
        "pooled connections must all be distinct objects -- a duplicate id "
        "means the same connection was published to the pool more than once"
    )


class _TagCollector(HTMLParser):
    def __init__(self, tag_name: str) -> None:
        super().__init__()
        self._tag_name = tag_name
        self.matches: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == self._tag_name:
            self.matches.append({key: value or "" for key, value in attrs})


def collect_tags(html: str, tag_name: str) -> list[dict[str, str]]:
    """Return the attribute dict of every ``<tag_name>`` element in ``html``,
    in document order.
    """
    parser = _TagCollector(tag_name)
    parser.feed(html)
    return parser.matches


class _DivNestingParser(HTMLParser):
    """Tracks ``<div>`` open/close depth to answer one question: is the
    element with ``id == target_id`` nested inside a ``<div>`` carrying
    ``ancestor_attr``?
    """

    def __init__(self, target_id: str, ancestor_attr: str) -> None:
        super().__init__()
        self._target_id = target_id
        self._ancestor_attr = ancestor_attr
        self._open_ancestor_flags: list[bool] = []
        self.found = False
        self.is_descendant = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "div":
            return
        attr_map = {key: value or "" for key, value in attrs}
        if attr_map.get("id") == self._target_id:
            self.found = True
            if any(self._open_ancestor_flags):
                self.is_descendant = True
        self._open_ancestor_flags.append(self._ancestor_attr in attr_map)

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self._open_ancestor_flags:
            self._open_ancestor_flags.pop()


def assert_summary_mount_not_nested_in_chart(html: str, *, summary_id: str) -> None:
    """Assert ``<div id=summary_id>`` exists in ``html`` and is never a
    descendant of a ``[data-chart]`` container.

    Chart containers get ``innerHTML = ""`` on every client-side render, so a
    summary mount living inside one would be destroyed on first paint -- this
    must hold at the template level, not just happen to be true today.
    """
    parser = _DivNestingParser(summary_id, "data-chart")
    parser.feed(html)
    assert parser.found, f'no <div id="{summary_id}"> found in the rendered HTML'
    assert not parser.is_descendant, (
        f'<div id="{summary_id}"> is nested inside a [data-chart] container -- '
        "it must be a sibling"
    )
