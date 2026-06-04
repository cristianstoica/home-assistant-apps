# pyright: strict
"""Durable storage sink: explicit append+flush, daily UTC rotation, gzip,
filename-date retention. Raises a domain `WriteError` on any failure.

Why not ``logging.handlers.TimedRotatingFileHandler``: the stdlib ``logging``
framework catches ``emit()`` / ``doRollover()`` failures *inside* the handler and
routes them to ``handleError`` (a stderr print, then return) — so a caller-side
``try/except OSError`` never fires and a failed disk write would still be counted
as delivered. This sink instead raises `WriteError` (wrapping the ``OSError``) so
the caller (`server.py`) owns the reliability decision: count the failure
separately, throttle-warn, keep receiving.

Durability model (the two paths differ on purpose):
  * per-line storage is ``write()`` + ``flush()`` — **not** per-line ``fsync``.
    ``flush`` hands the line to the OS page cache; a process crash keeps every
    flushed line, a power-loss can lose the most recent un-fsynced lines. That
    is the right tradeoff for a UDP firehose (UDP is already lossy; per-line
    fsync would throttle the stream).
  * closed archives are fsynced and crash-durable: the atomic gzip path is
    write -> fsync ``*.gz.tmp`` -> ``os.replace`` -> fsync ``log_dir`` -> unlink
    source -> fsync ``log_dir``, so the ``.gz`` name is durable before the source
    is removed (no power-loss gap between "rename persisted" and "source
    deleted").

Retention prunes by the ``<date>`` **embedded in the filename**, not by mtime:
reconciliation re-gzips a stale orphan with a *fresh* mtime that mtime-based
pruning would wrongly let escape.
"""

from __future__ import annotations

import gzip
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, TextIO

# syslog.log.<YYYY-MM-DD>.gz  (and its .tmp partial, and the uncompressed orphan)
_ARCHIVE_RE = re.compile(r"^(?P<base>.+)\.(?P<date>\d{4}-\d{2}-\d{2})\.gz$")
_UNCOMPRESSED_RE = re.compile(r"^(?P<base>.+)\.(?P<date>\d{4}-\d{2}-\d{2})$")
_TMP_RE = re.compile(r"^(?P<base>.+)\.(?P<date>\d{4}-\d{2}-\d{2})\.gz\.tmp$")


class WriteError(Exception):
    """Domain error for any storage write/rotation failure (wraps ``OSError``)."""


def _utc_now() -> datetime:
    """Default injectable clock: current UTC time."""
    return datetime.now(timezone.utc)


def _fsync_dir(path: Path) -> None:
    """fsync a directory so a contained rename/unlink is durable."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Writer:
    """Append-mode storage sink with daily UTC rotation, gzip, and retention.

    `now` is an injectable clock (defaults to UTC now) so rollover is
    deterministic under test without waiting for real midnight. Construction
    creates `log_dir`, runs startup reconciliation, prunes, and opens the active
    base file — any failure there raises `WriteError` and is fatal at startup.
    """

    def __init__(
        self,
        log_dir: str,
        log_file: str,
        retention_days: int,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._dir = Path(log_dir)
        self._base_name = log_file
        self._retention_days = retention_days
        self._now = now
        self._handle: TextIO | None = None
        self._open_date: date = now().date()

        self._ensure_dir()
        self._reconcile()
        self._prune()
        self._open(self._open_date)

    # --- paths ---------------------------------------------------------------

    @property
    def _base_path(self) -> Path:
        return self._dir / self._base_name

    def _archive_path(self, day: date) -> Path:
        return self._dir / f"{self._base_name}.{day.isoformat()}.gz"

    def _uncompressed_path(self, day: date) -> Path:
        return self._dir / f"{self._base_name}.{day.isoformat()}"

    def _tmp_path(self, day: date) -> Path:
        return self._dir / f"{self._base_name}.{day.isoformat()}.gz.tmp"

    # --- startup -------------------------------------------------------------

    def _ensure_dir(self) -> None:
        """Create `log_dir` (parents) at runtime; failure is fatal at startup.

        The HA ``/data`` mount shadows any image-baked directory, so the dir
        must exist before reconciliation/open. A regular file at the path yields
        ``ENOTDIR`` here, surfaced as a fatal `WriteError`.
        """
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise WriteError(f"cannot create log_dir {self._dir}: {exc}") from exc
        if not self._dir.is_dir():
            raise WriteError(f"log_dir {self._dir} is not a directory")

    def _reconcile(self) -> None:
        """Make bounded retention crash-safe before the first append.

        Handles the four stale states: a prior-UTC-day active base, orphaned
        uncompressed ``syslog.log.<date>``, stale ``*.gz.tmp`` partials, and a
        source+final-``.gz`` pair (crash after ``os.replace`` before unlink).
        """
        try:
            self._reconcile_stale_base()
            self._reconcile_orphans()
        except OSError as exc:
            raise WriteError(f"reconciliation failed: {exc}") from exc

    def _reconcile_stale_base(self) -> None:
        """Rotate a leftover base whose mtime falls on a previous UTC day."""
        base = self._base_path
        if not base.exists():
            return
        mtime = datetime.fromtimestamp(base.stat().st_mtime, tz=timezone.utc)
        mtime_day = mtime.date()
        if mtime_day < self._now().date():
            self._compress(base, mtime_day)

    def _reconcile_orphans(self) -> None:
        """Resolve orphaned uncompressed files, tmp partials, and dup pairs."""
        for child in sorted(self._dir.iterdir()):
            name = child.name
            tmp = _TMP_RE.match(name)
            if tmp is not None and tmp.group("base") == self._base_name:
                # Partial gzip: drop it; the surviving source (if any) re-gzips.
                child.unlink()
                continue
            uncompressed = _UNCOMPRESSED_RE.match(name)
            if (
                uncompressed is not None
                and uncompressed.group("base") == self._base_name
            ):
                day = date.fromisoformat(uncompressed.group("date"))
                final = self._archive_path(day)
                if final.exists():
                    # Crash after os.replace, before unlink: source is redundant.
                    child.unlink()
                else:
                    self._compress(child, day)

    def _prune(self) -> None:
        """Delete archives whose embedded ``<date>`` is beyond retention.

        Keyed on the **filename date**, not mtime — a just-re-gzipped orphan
        carries a fresh mtime that mtime-pruning would wrongly spare.
        """
        cutoff = self._now().date()
        try:
            for child in sorted(self._dir.iterdir()):
                match = _ARCHIVE_RE.match(child.name)
                if match is None or match.group("base") != self._base_name:
                    continue
                day = date.fromisoformat(match.group("date"))
                if (cutoff - day).days > self._retention_days:
                    child.unlink()
        except OSError as exc:
            raise WriteError(f"prune failed: {exc}") from exc

    # --- active file ---------------------------------------------------------

    def _open(self, day: date) -> None:
        """Open the active base file in append mode for UTC `day`."""
        try:
            self._handle = self._base_path.open("a", encoding="utf-8")
        except OSError as exc:
            raise WriteError(f"cannot open {self._base_path}: {exc}") from exc
        self._open_date = day

    def _rotate_if_needed(self) -> None:
        """Roll over to a new day's base if the UTC date advanced."""
        today = self._now().date()
        if today == self._open_date:
            return
        if self._handle is not None:
            self._handle.close()
            self._handle = None
        prior = self._open_date
        if self._base_path.exists():
            self._compress(self._base_path, prior)
        self._prune()
        self._open(today)

    def _compress(self, source: Path, day: date) -> None:
        """Atomically gzip `source` into ``<base>.<day>.gz``.

        write -> fsync ``*.gz.tmp`` -> ``os.replace`` -> fsync ``log_dir`` ->
        unlink source -> fsync ``log_dir``. A crash mid-compress never leaves a
        truncated ``.gz`` masquerading as complete (the tmp is unnamed until the
        rename), and the ``.gz`` name is durable before the source is removed.

        v1 assumes **one archive per UTC day**: a second compress for the same
        `day` would ``os.replace``-clobber the existing ``.gz`` (only reachable
        via a rare reconcile-then-rollover on the same day). Accepted for the
        single low-volume sender; revisit if a same-day collision becomes real.
        """
        final = self._archive_path(day)
        tmp = self._tmp_path(day)
        with source.open("rb") as src, gzip.open(tmp, "wb") as dst:
            while True:
                chunk = src.read(65536)
                if not chunk:
                    break
                dst.write(chunk)
        with tmp.open("rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, final)
        _fsync_dir(self._dir)
        source.unlink()
        _fsync_dir(self._dir)

    # --- public API (WriterProtocol) -----------------------------------------

    def write(self, line: str) -> None:
        """Append one line (already ``\\n``-terminated): write + flush.

        Rotates first if the UTC day advanced. Any I/O failure is wrapped in a
        `WriteError` so the caller counts it separately and keeps receiving.
        """
        try:
            self._rotate_if_needed()
            if self._handle is None:
                self._open(self._now().date())
            assert self._handle is not None
            self._handle.write(line)
            self._handle.flush()
        except OSError as exc:
            raise WriteError(f"write failed: {exc}") from exc

    def close(self) -> None:
        """Flush and close the active base file (best-effort on shutdown)."""
        if self._handle is not None:
            try:
                self._handle.flush()
                self._handle.close()
            except OSError:
                pass
            self._handle = None
