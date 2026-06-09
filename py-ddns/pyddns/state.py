# pyright: strict
"""Persist the last-known IP under ``/data`` (atomic write, behind a seam).

The updater gates persistence on this seam: it writes the last-known value **only
on a confirmed success** (an API 2xx PUT, or a callback fire confirmed by a
post-fire resolve), so a crash mid-update or a 2xx-without-effect can never
desync local state. The first cycle on start is always authoritative regardless
of what is stored, so a stale value here never suppresses a startup self-heal.

The write is atomic (``*.tmp`` → ``os.replace``) so a crash mid-write leaves the
prior value intact rather than a truncated file. A missing/unreadable/garbage
state file reads back as ``None`` (no last-known) — never an error.
"""

from __future__ import annotations

import logging
import os
from ipaddress import AddressValueError, IPv4Address
from pathlib import Path
from typing import Protocol

_log = logging.getLogger("pyddns")


class State(Protocol):
    """Last-known-IP persistence seam (real file store + the oracle's in-memory fake)."""

    def read(self) -> IPv4Address | None: ...

    def write(self, value: IPv4Address) -> None: ...


class FileState:
    """A `State` backed by a single small file under ``/data``."""

    def __init__(self, path: str) -> None:
        self._path = Path(path)

    def read(self) -> IPv4Address | None:
        """Return the persisted IPv4, or ``None`` if absent/unreadable/garbage."""
        try:
            text = self._path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if text == "":
            return None
        try:
            return IPv4Address(text)
        except (AddressValueError, ValueError):
            _log.warning("last-known-ip state file holds a non-IPv4 value; ignoring")
            return None

    def write(self, value: IPv4Address) -> None:
        """Atomically persist `value` (``*.tmp`` → fsync → ``os.replace``)."""
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(str(value))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._path)
