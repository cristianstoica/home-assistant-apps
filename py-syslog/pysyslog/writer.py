# pyright: strict
"""Durable storage sink: explicit append+flush, daily UTC rotation, gzip,
filename-date retention, and an optional size-bounded ring buffer. Raises a
domain `WriteError` on any durability failure.

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

Size guard (the deliberate failure-handling asymmetry): the optional
``max_segment_mb`` / ``min_free_percent`` / ``max_log_percent`` knobs add a
size-bounded ring buffer on top of time-retention. The guard is a *monitoring*
concern, not a *durability* one, so its measurement/decision code is
**asymmetric** from the ``write()`` path: ``_enforce_space`` and its measurement
helpers (`_volume_stats` / `_log_dir_bytes`) catch their own ``OSError``, emit a
throttled WARNING through the injectable ``warn`` seam, and return without
pruning (retry next roll/tick) — the collector never crashes on a guard
measurement failure. A failure *inside* ``_compress_segment`` (the gzip itself)
DOES raise `WriteError`, because that is a real durability op, identical to
``_compress``. **Do not "fix" the measurement guard into a raise** — degrading
silently-with-warning is intentional. The guard also **never** deletes the
active base file to make room: if pruning every rotated segment still cannot
satisfy the floor, the correct outcome is the counted `WriteError` path, not
silent live-data loss.
"""

from __future__ import annotations

import gzip
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, TextIO

# syslog.log.<YYYY-MM-DD>.gz  (and its .tmp partial, and the uncompressed orphan).
# The trailing ``\.gz$`` anchor keeps these matching the *daily* form only — a
# numbered segment (``<date>.<NNN>.gz``) never matches the archive/uncompressed/
# tmp trio.
_ARCHIVE_RE = re.compile(r"^(?P<base>.+)\.(?P<date>\d{4}-\d{2}-\d{2})\.gz$")
_UNCOMPRESSED_RE = re.compile(r"^(?P<base>.+)\.(?P<date>\d{4}-\d{2}-\d{2})$")
_TMP_RE = re.compile(r"^(?P<base>.+)\.(?P<date>\d{4}-\d{2}-\d{2})\.gz\.tmp$")

# Size-rotation segments mirror the daily trio with a 3-digit per-UTC-day
# sequence: syslog.log.<YYYY-MM-DD>.<NNN>.gz (+ its uncompressed orphan + tmp).
_SEG_RE = re.compile(r"^(?P<base>.+)\.(?P<date>\d{4}-\d{2}-\d{2})\.(?P<seq>\d{3})\.gz$")
_SEG_UNCOMPRESSED_RE = re.compile(
    r"^(?P<base>.+)\.(?P<date>\d{4}-\d{2}-\d{2})\.(?P<seq>\d{3})$"
)
_SEG_TMP_RE = re.compile(
    r"^(?P<base>.+)\.(?P<date>\d{4}-\d{2}-\d{2})\.(?P<seq>\d{3})\.gz\.tmp$"
)

_MAX_SEQ = 999
_MB = 1024 * 1024


class WriteError(Exception):
    """Domain error for any storage write/rotation failure (wraps ``OSError``)."""


class SizeGuardStats:
    """Mutable size-guard counters owned by the ``Writer``.

    The Writer owns its own stats object (it is the only code that knows when a
    size-roll or space-prune happened); `server.py` reads this snapshot at
    stats-emit time. Keeping it here preserves the Writer's self-contained
    state-machine boundary — `process_datagram`'s fixed counter order is
    untouched.
    """

    def __init__(self) -> None:
        self.size_rotations = 0
        self.space_prunes = 0
        self.bytes_reclaimed = 0


def _utc_now() -> datetime:
    """Default injectable clock: current UTC time."""
    return datetime.now(timezone.utc)


def _real_volume_stats(path: Path) -> tuple[int, int]:
    """Default injectable measurement: ``(total_bytes, free_bytes)`` of `path`.

    ``total = f_blocks * f_frsize``; ``free = f_bavail * f_frsize`` — ``f_bavail``
    is the honest figure usable by an unprivileged writer (excludes the
    root-reserved blocks ``f_bfree`` would include).
    """
    st = os.statvfs(path)
    total = st.f_blocks * st.f_frsize
    free = st.f_bavail * st.f_frsize
    return (total, free)


def _segment_sort_key(name: str) -> tuple[date, int]:
    """Order key for a ``.gz`` archive name: ``(embedded date, sequence)``.

    The bare daily form ``<base>.<date>.gz`` keys ``(date, -1)`` — older than any
    same-day numbered segment (``seq`` starts at ``001``), so the daily archive
    prunes first within a day (keep-newest). A numbered segment keys
    ``(date, int(seq))``. Callers filter to matching ``.gz`` names first; an
    unmatched name raises ``ValueError`` (a programming error, never reached).
    """
    seg = _SEG_RE.match(name)
    if seg is not None:
        return (date.fromisoformat(seg.group("date")), int(seg.group("seq")))
    daily = _ARCHIVE_RE.match(name)
    if daily is not None:
        return (date.fromisoformat(daily.group("date")), -1)
    raise ValueError(f"not a recognized archive name: {name!r}")


# Public alias: the ``--check --storage`` oracle pins the ``(date, seq)`` order
# directly on hand-built names. Exposed under a public name so the cross-module
# call is not a private-usage access (mirrors ``server.trace_datagram``).
segment_sort_key = _segment_sort_key


def _fsync_dir(path: Path) -> None:
    """fsync a directory so a contained rename/unlink is durable."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _default_warn(_key: str, message: str) -> None:
    """Default guard warner: an un-throttled ``print`` to stderr.

    The live server passes its **throttled** ``_warn_throttled`` so a flood
    cannot warn at segment-roll rate; the oracle injects a recording stub. This
    default keeps a standalone Writer usable without wiring a throttle. The
    ``key`` is ignored here (it only matters to a throttle).
    """
    import sys

    print(message, file=sys.stderr)


class Writer:
    """Append-mode storage sink with daily UTC rotation, gzip, retention, and an
    optional size-bounded ring buffer.

    `now` is an injectable clock (defaults to UTC now) so rollover is
    deterministic under test without waiting for real midnight. `warn` is the
    injectable guard-warning seam (default un-throttled stderr; the live server
    passes its throttled warner). `volume_stats` is the injectable
    free/total-bytes measurement (default real ``os.statvfs``) so the guard
    logic can be driven against a faked volume without real disk pressure.

    The size-guard knobs all default to ``0`` (disabled), so the existing oracle
    ``Writer(...)`` call sites exercise the 1.2.0 path unchanged; the live source
    (`Config`) always passes explicit values.

    Construction creates `log_dir`, runs startup reconciliation, prunes, and
    opens the active base file — any failure there raises `WriteError` and is
    fatal at startup.
    """

    def __init__(
        self,
        log_dir: str,
        log_file: str,
        retention_days: int,
        now: Callable[[], datetime] = _utc_now,
        *,
        min_free_percent: int = 0,
        max_log_percent: int = 0,
        max_segment_mb: int = 0,
        warn: Callable[[str, str], None] = _default_warn,
        volume_stats: Callable[[Path], tuple[int, int]] = _real_volume_stats,
    ) -> None:
        self._dir = Path(log_dir)
        self._base_name = log_file
        self._retention_days = retention_days
        self._now = now
        self._min_free_percent = min_free_percent
        self._max_log_percent = max_log_percent
        self._max_segment_mb = max_segment_mb
        self._warn = warn
        self._volume_stats_fn = volume_stats
        self._handle: TextIO | None = None
        self._open_date: date = now().date()
        self.stats = SizeGuardStats()

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

    def _segment_path(self, day: date, seq: int) -> Path:
        return self._dir / f"{self._base_name}.{day.isoformat()}.{seq:03d}.gz"

    def _segment_tmp_path(self, day: date, seq: int) -> Path:
        return self._dir / f"{self._base_name}.{day.isoformat()}.{seq:03d}.gz.tmp"

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

        Handles the stale states: a prior-UTC-day active base, orphaned
        uncompressed ``syslog.log.<date>`` (daily and numbered), stale
        ``*.gz.tmp`` partials (daily and numbered), and a source+final-``.gz``
        pair (crash after ``os.replace`` before unlink).
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
        """Resolve orphaned uncompressed files, tmp partials, and dup pairs.

        Both the daily form (``<date>``) and the numbered size-segment form
        (``<date>.<NNN>``) are handled identically: a stale ``*.gz.tmp`` partial
        is dropped; an uncompressed orphan is re-gzipped unless its final
        ``.gz`` already exists (crash after ``os.replace`` before unlink), in
        which case the redundant source is removed.
        """
        for child in sorted(self._dir.iterdir()):
            name = child.name
            seg_tmp = _SEG_TMP_RE.match(name)
            if seg_tmp is not None and seg_tmp.group("base") == self._base_name:
                child.unlink()
                continue
            tmp = _TMP_RE.match(name)
            if tmp is not None and tmp.group("base") == self._base_name:
                # Partial gzip: drop it; the surviving source (if any) re-gzips.
                child.unlink()
                continue
            seg = _SEG_UNCOMPRESSED_RE.match(name)
            if seg is not None and seg.group("base") == self._base_name:
                day = date.fromisoformat(seg.group("date"))
                seq = int(seg.group("seq"))
                final = self._segment_path(day, seq)
                if final.exists():
                    child.unlink()
                else:
                    self._compress_segment(child, day, seq)
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
        carries a fresh mtime that mtime-pruning would wrongly spare. Both the
        daily form and the numbered size-segment form (which embeds the same
        ``<date>``) are pruned by the same date rule.
        """
        cutoff = self._now().date()
        try:
            for child in sorted(self._dir.iterdir()):
                name = child.name
                seg = _SEG_RE.match(name)
                if seg is not None and seg.group("base") == self._base_name:
                    day = date.fromisoformat(seg.group("date"))
                    if (cutoff - day).days > self._retention_days:
                        child.unlink()
                    continue
                match = _ARCHIVE_RE.match(name)
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
        self._atomic_gzip(source, self._tmp_path(day), self._archive_path(day))

    def _compress_segment(self, source: Path, day: date, seq: int) -> None:
        """Atomically gzip `source` into ``<base>.<day>.<NNN>.gz``.

        Identical atomic path to `_compress` (a real durability op, so an
        ``OSError`` here propagates as `WriteError`). The numbered name never
        clobbers: ``seq`` is monotonic within the UTC day, so the final ``.gz``
        cannot pre-exist for a fresh roll.
        """
        self._atomic_gzip(
            source, self._segment_tmp_path(day, seq), self._segment_path(day, seq)
        )

    def _atomic_gzip(self, source: Path, tmp: Path, final: Path) -> None:
        """Shared atomic gzip: write tmp -> fsync -> replace -> fsync dir ->
        unlink source -> fsync dir. Used by both daily and numbered compress."""
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

    # --- size guard ----------------------------------------------------------

    def _volume_stats(self) -> tuple[int, int]:
        """``(total_bytes, free_bytes)`` for the log volume via the injected fn."""
        return self._volume_stats_fn(self._dir)

    def _log_dir_bytes(self) -> int:
        """Sum the byte sizes of regular files directly in `log_dir`.

        Skips symlinks and subdirectories (``entry.is_file(follow_symlinks=False)``)
        so the figure is the real on-disk weight of the log set, never a followed
        link or a nested tree.
        """
        total = 0
        with os.scandir(self._dir) as it:
            for entry in it:
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat().st_size
        return total

    def _next_seq(self, day: date) -> int:
        """Next free size-segment sequence for `day` (highest existing + 1).

        Scans existing ``<base>.<day>.<NNN>.gz`` segments after reconcile, so a
        mid-day restart continues at the next free seq with no collision. Returns
        ``1`` when no same-day segment exists.
        """
        highest = 0
        for child in self._dir.iterdir():
            seg = _SEG_RE.match(child.name)
            if seg is None or seg.group("base") != self._base_name:
                continue
            if date.fromisoformat(seg.group("date")) != day:
                continue
            highest = max(highest, int(seg.group("seq")))
        return highest + 1

    def _rotate_by_size_if_needed(self) -> None:
        """Roll the active base to a numbered ``.gz`` segment when it exceeds
        ``max_segment_mb``, then enforce the space budget.

        Called from `write()` after the daily-rollover check and a successful
        write+flush, so the size check is on the post-flush byte offset (a cheap
        ``tell()``, no ``stat``). The active file may exceed the threshold by at
        most one datagram (≤ 64 KB) before rolling — negligible vs MB-scale
        segments. A real gzip/replace failure here propagates as `WriteError`
        (durability op); the subsequent `_enforce_space` degrades safe.
        """
        if self._max_segment_mb == 0:
            return
        assert self._handle is not None
        if self._handle.tell() < self._max_segment_mb * _MB:
            return
        today = self._open_date
        seq = self._next_seq(today)
        if seq > _MAX_SEQ:
            # Seq-overflow terminal: skip the roll, keep appending to the active
            # file for the rest of the UTC day; the daily rollover resets seq.
            self._warn(
                "space:seq-overflow",
                f"size guard: {self._base_name}.{today.isoformat()} reached "
                f"{_MAX_SEQ} segments; skipping roll, appending to active file",
            )
            return
        self._handle.close()
        self._handle = None
        try:
            self._compress_segment(self._base_path, today, seq)
        except OSError as exc:
            raise WriteError(f"size-rotation failed: {exc}") from exc
        self._open(today)
        self.stats.size_rotations += 1
        self._enforce_space()

    def _enforce_space(self) -> None:
        """Two-dimensional ring buffer: prune oldest segments until the free-space
        floor and the log-dir cap both hold.

        Degrade-safe: a measurement ``OSError`` emits a throttled WARNING and
        returns (retry next roll/tick) — it never raises. Re-measures every
        iteration (a concurrent daily rollover or write can change both figures);
        cheap and only on the over-budget path. Never deletes the active base —
        if pruning every rotated segment still can't satisfy the floor, it stops
        with one ``space:exhausted`` WARNING and lets the counted `WriteError`
        path take over.
        """
        if self._min_free_percent == 0 and self._max_log_percent == 0:
            return
        try:
            total, _free = self._volume_stats()
        except OSError as exc:
            self._warn("space:statvfs", f"size guard: statvfs failed: {exc}")
            return
        while True:
            try:
                _total, free = self._volume_stats()
                logsz = self._log_dir_bytes()
            except OSError as exc:
                self._warn("space:scandir", f"size guard: measurement failed: {exc}")
                return
            need_free = (
                self._min_free_percent > 0
                and free < total * self._min_free_percent // 100
            )
            over_cap = (
                self._max_log_percent > 0
                and logsz > total * self._max_log_percent // 100
            )
            if not (need_free or over_cap):
                return
            victim = self._oldest_prunable_segment()
            if victim is None:
                self._warn(
                    "space:exhausted",
                    "size guard: only the active file remains and still over "
                    "budget; not pruning live data (WriteError path takes over)",
                )
                return
            try:
                reclaimed = victim.stat().st_size
                victim.unlink()
                _fsync_dir(self._dir)
            except OSError as exc:
                self._warn("space:unlink", f"size guard: prune failed: {exc}")
                return
            self.stats.space_prunes += 1
            self.stats.bytes_reclaimed += reclaimed
            free_pct = (free * 100 // total) if total > 0 else 0
            log_pct = (logsz * 100 // total) if total > 0 else 0
            self._warn(
                "space:prune",
                f"space guard pruned {victim.name} "
                f"(disk_free={free_pct}% log_dir={log_pct}%); kept newest",
            )

    def _oldest_prunable_segment(self) -> Path | None:
        """The ``(date, seq)``-minimum ``.gz`` archive (daily or numbered).

        Excludes the active base file by construction (only ``.gz`` archives
        match), so the ring buffer is keep-newest / drop-oldest and never
        deletes live data. Returns ``None`` when no rotated segment remains.
        """
        candidates: list[Path] = []
        for child in self._dir.iterdir():
            name = child.name
            seg = _SEG_RE.match(name)
            if seg is not None and seg.group("base") == self._base_name:
                candidates.append(child)
                continue
            daily = _ARCHIVE_RE.match(name)
            if daily is not None and daily.group("base") == self._base_name:
                candidates.append(child)
        if not candidates:
            return None
        return min(candidates, key=lambda p: _segment_sort_key(p.name))

    def enforce_space_tick(self) -> None:
        """Public stats-tick backstop: run `_enforce_space` between size-rolls.

        The size-roll path is the primary, write-driven trigger; this lets the
        periodic stats tick re-check the budget if a non-log file grew the volume
        without any roll occurring. A no-op when the guard is disabled.
        """
        self._enforce_space()

    def disk_free_pct(self) -> int | None:
        """Live gauge: free space as a whole-percent of the volume, or ``None``.

        Returns ``None`` on a measurement failure so the stats line renders
        ``disk_free_pct=?`` rather than crashing. Always available (the gauges
        render even when the guard is disabled).
        """
        try:
            total, free = self._volume_stats()
        except OSError:
            return None
        if total <= 0:
            return None
        return free * 100 // total

    def log_dir_mb(self) -> int | None:
        """Live gauge: log-dir size in whole MB, or ``None`` on failure."""
        try:
            return self._log_dir_bytes() // _MB
        except OSError:
            return None

    # --- public API (WriterProtocol) -----------------------------------------

    def write(self, line: str) -> None:
        """Append one line (already ``\\n``-terminated): write + flush.

        Rotates first if the UTC day advanced, then (after a successful
        write+flush) rolls to a numbered segment if the active file crossed
        ``max_segment_mb`` and enforces the space budget. Any I/O failure on the
        write/daily-rotation/size-rotation path is wrapped in a `WriteError` so
        the caller counts it separately and keeps receiving; the space-guard
        *measurement* degrades safe (warns, no raise).
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
        self._rotate_by_size_if_needed()

    def close(self) -> None:
        """Flush and close the active base file (best-effort on shutdown)."""
        if self._handle is not None:
            try:
                self._handle.flush()
                self._handle.close()
            except OSError:
                pass
            self._handle = None
