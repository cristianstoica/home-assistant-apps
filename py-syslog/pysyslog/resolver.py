# pyright: strict
"""IP -> (site, host) resolution with warn-once on unknown senders.

A `Resolver` is a per-run object (no module globals), so the warn-once
`seen_unknown` set is owned by the instance and the `--check` oracle can build a
fresh resolver with no leakage between runs.
"""

from __future__ import annotations

import logging

from .models import SourceMapping

_log = logging.getLogger("pysyslog")


class Resolver:
    """Resolve a sender IP to its configured ``(site, host)``.

    A configured IP resolves to its mapping; an unconfigured IP resolves to
    ``("unknown", ip)`` and triggers exactly one WARNING the first time that IP
    is seen (subsequent datagrams from it are silent).
    """

    def __init__(self, sources: dict[str, SourceMapping]) -> None:
        self._sources = sources
        self.seen_unknown: set[str] = set()

    def resolve(self, ip: str) -> tuple[str, str]:
        """Return ``(site, host)`` for `ip`; ``("unknown", ip)`` on a miss.

        Warns once per previously-unseen unknown IP. This is the only side
        effect; the return value is otherwise pure in the configured mapping.
        """
        mapping = self._sources.get(ip)
        if mapping is not None:
            return (mapping.site, mapping.host)
        if ip not in self.seen_unknown:
            self.seen_unknown.add(ip)
            _log.warning("syslog from unknown source %s -> stamped unknown/%s", ip, ip)
        return ("unknown", ip)

    def is_known(self, ip: str) -> bool:
        """True iff `ip` is a configured source. The authoritative miss signal —
        callers must not infer a miss from the rendered ``site == "unknown"``,
        which a configured source may legally use as its label."""
        return ip in self._sources

    def note_unknown_rejected(self, ip: str) -> None:
        """Warn-once that an unknown-source datagram from `ip` was rejected.
        Shares resolve()'s seen_unknown set so each unknown IP warns at most once
        across BOTH paths; side-effect only, no stamping claim (datagram is dropped)."""
        if ip not in self.seen_unknown:
            self.seen_unknown.add(ip)
            _log.warning(
                "rejected datagram from unknown source %s (reject_unknown_sources enabled)",
                ip,
            )
