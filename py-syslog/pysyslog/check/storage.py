# pyright: strict
"""The ``--check --storage`` Writer state-machine exercise.

``check_storage`` is moved verbatim from the inline oracle: every nested fake
clock/volume class stays nested, and the only edits versus the original are the
``.writer`` -> ``..writer`` import path, a module-level ``import sys``, and the
``RecordingWarn`` fake imported from ``check/fakes.py``.
"""

from __future__ import annotations

import sys

from .fakes import RecordingWarn


def check_storage() -> bool:
    """Exercise the real `Writer` state machine deterministically.

    Each sub-check runs in its own tempdir with an injected fake clock (no real
    midnight wait): rollover, gzip atomicity + contents, prune-by-filename-date
    (incl. the re-gzipped-fresh-mtime escape case), four-state reconciliation,
    and the ENOTDIR startup-fatal path.

    Cases A–I add the size guard: size-rotation into numbered segments (A),
    ``(date, seq)`` prune ordering / keep-newest (B), both-constraint pruning
    (C), restart-safe next-seq (D), ring-buffer keep-newest end-to-end (E), the
    only-active-file terminal (F), statvfs-failure degrade-safe (G), the
    seq-overflow terminal (H), and guard-disabled == 1.2.0 (I). Cases J–M close
    the audited branch gaps: numbered-orphan reconciliation (J), next-seq scans
    AFTER reconcile (K), the in-loop ``space:scandir`` measurement-failure branch
    distinct from the pre-loop ``space:statvfs`` (M1), the ``space:unlink``
    prune-failure branch (M2), and the live ``disk_free_pct`` / ``log_dir_mb``
    gauges incl. their None paths and regular-files-only byte sum (M3/M4). L2/L3
    pin a single-oversized-line roll and byte-exact segment contents. Size-rolls
    are driven deterministically by writing fixed large lines past a low
    ``max_segment_mb``; the free-space dimension is driven via the injectable
    ``volume_stats`` seam (no real disk pressure). Returns ``True`` only when all
    pass.
    """
    import gzip
    import os
    import tempfile
    from datetime import datetime, timedelta, timezone
    from pathlib import Path

    from ..writer import WriteError, Writer, segment_sort_key

    results: list[tuple[str, bool]] = []

    def _record(label: str, passed: bool) -> None:
        results.append((label, passed))
        print(f"{'PASS' if passed else 'FAIL'}  {label}", file=sys.stderr)

    class _FakeClock:
        def __init__(self, start: datetime) -> None:
            self.value = start

        def __call__(self) -> datetime:
            return self.value

    class _FakeVolume:
        """Mutable ``(total, free)`` stand-in for the injectable volume_stats seam.

        ``free`` can be re-pinned mid-test so a prune (which the guard re-measures
        as freeing space) can be modeled, and ``total`` fixed so the percentage
        budgets are deterministic without real disk pressure.
        """

        def __init__(self, total: int, free: int) -> None:
            self.total = total
            self.free = free

        def __call__(self, _path: Path) -> tuple[int, int]:
            return (self.total, self.free)

    class _RaisingVolume:
        """An injectable volume_stats seam that always raises ``OSError``."""

        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, _path: Path) -> tuple[int, int]:
            self.calls += 1
            raise OSError("injected statvfs failure")

    # A 128 KiB line: 8 writes == exactly 1 MiB, so a Writer with
    # ``max_segment_mb=1`` rolls on every 8th write — deterministic crossings
    # with no real disk pressure.
    _LINE_128K = "x" * (128 * 1024 - 1) + "\n"

    day0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    # (1) rollover + (2) gzip atomicity + contents.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=30, now=clock)
        w.write("day0 line A\n")
        w.write("day0 line B\n")
        clock.value = day0 + timedelta(days=1)
        w.write("day1 line C\n")
        w.close()
        d = Path(tmp)
        archive = d / "syslog.log.2026-06-01.gz"
        base = d / "syslog.log"
        tmp_partials = list(d.glob("*.gz.tmp"))
        _record("rollover: prior-day archive exists", archive.is_file())
        _record(
            "rollover: active base holds only the new day",
            base.read_text(encoding="utf-8") == "day1 line C\n",
        )
        if archive.is_file():
            with gzip.open(archive, "rt", encoding="utf-8") as fh:
                content = fh.read()
            _record(
                "gzip contents == day0 lines",
                content == "day0 line A\nday0 line B\n",
            )
        else:
            _record("gzip contents == day0 lines", False)
        _record("no *.gz.tmp partial remains", tmp_partials == [])

    # (3) prune-by-filename-date, incl. a re-gzipped orphan with a fresh mtime.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # An old archive far beyond retention (embedded date wins over mtime).
        old_archive = d / "syslog.log.2026-01-01.gz"
        with gzip.open(old_archive, "wt", encoding="utf-8") as fh:
            fh.write("ancient\n")
        # An orphaned *uncompressed* old file: reconciliation re-gzips it with a
        # FRESH mtime; prune must still drop it by its embedded date.
        orphan = d / "syslog.log.2026-01-02"
        orphan.write_text("orphan\n", encoding="utf-8")
        clock = _FakeClock(day0)
        Writer(tmp, "syslog.log", retention_days=30, now=clock).close()
        gone_archive = not old_archive.exists()
        regzipped = d / "syslog.log.2026-01-02.gz"
        gone_orphan_archive = not regzipped.exists() and not orphan.exists()
        _record("prune drops far-past archive by embedded date", gone_archive)
        _record(
            "prune drops re-gzipped orphan by embedded date (mtime-escape case)",
            gone_orphan_archive,
        )

    # (4) four-state reconciliation seeded at once.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        prior_day = (day0 - timedelta(days=1)).date()
        # (a) prior-day active base
        base = d / "syslog.log"
        base.write_text("stale active\n", encoding="utf-8")
        stale_ts = (day0 - timedelta(days=1)).timestamp()
        os.utime(base, (stale_ts, stale_ts))
        # (b) orphaned uncompressed file (recent enough to survive prune)
        orphan_day = (day0 - timedelta(days=2)).date()
        orphan = d / f"syslog.log.{orphan_day.isoformat()}"
        orphan.write_text("orphan body\n", encoding="utf-8")
        # (c) stale *.gz.tmp partial
        partial = d / "syslog.log.2026-05-20.gz.tmp"
        partial.write_bytes(b"partial garbage")
        # (d) source + final .gz pair (crash after replace, before unlink)
        pair_day = (day0 - timedelta(days=3)).date()
        pair_source = d / f"syslog.log.{pair_day.isoformat()}"
        pair_source.write_text("dup source\n", encoding="utf-8")
        pair_final = d / f"syslog.log.{pair_day.isoformat()}.gz"
        with gzip.open(pair_final, "wt", encoding="utf-8") as fh:
            fh.write("final already\n")

        clock = _FakeClock(day0)
        Writer(tmp, "syslog.log", retention_days=30, now=clock).close()

        rotated = d / f"syslog.log.{prior_day.isoformat()}.gz"
        orphan_gz = d / f"syslog.log.{orphan_day.isoformat()}.gz"
        _record(
            "reconcile: stale base rotated to embedded-mtime-day", rotated.is_file()
        )
        _record("reconcile: orphan gzipped", orphan_gz.is_file())
        _record("reconcile: orphan source removed", not orphan.exists())
        _record("reconcile: *.gz.tmp partial deleted", not partial.exists())
        _record(
            "reconcile: dup source deleted, final kept",
            (not pair_source.exists()) and pair_final.is_file(),
        )
        _record(
            "reconcile: no *.gz.tmp partials remain", list(d.glob("*.gz.tmp")) == []
        )

    # (5) startup-fatal: log_dir pointing at a regular file -> WriteError (ENOTDIR).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        regular_file = d / "not_a_dir"
        regular_file.write_text("i am a file\n", encoding="utf-8")
        bad_dir = regular_file / "log"
        raised = False
        try:
            Writer(str(bad_dir), "syslog.log", retention_days=30)
        except WriteError:
            raised = True
        _record("startup-fatal: regular-file log_dir raises WriteError", raised)

    # === Size guard (Cases A–I) ===

    def _prune_victims(warn: RecordingWarn) -> list[str]:
        """The segment names pruned, in prune order, parsed from space:prune warns."""
        names: list[str] = []
        for key, message in warn.calls:
            if key == "space:prune":
                # "space guard pruned <name> (disk_free=..%...); kept newest"
                names.append(message.split("pruned ", 1)[1].split(" ", 1)[0])
        return names

    # (A) size-rotation produces numbered segments.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=30,
            now=clock,
            max_segment_mb=1,
            warn=warn,
        )
        for _ in range(16):  # 16 * 128 KiB == 2 MiB -> exactly two rolls
            w.write(_LINE_128K)
        w.write("post-002 tail\n")
        w.close()
        d = Path(tmp)
        seg1 = d / "syslog.log.2026-06-01.001.gz"
        seg2 = d / "syslog.log.2026-06-01.002.gz"
        base = d / "syslog.log"
        _record("A: size-roll segment 001 exists", seg1.is_file())
        _record("A: size-roll segment 002 exists", seg2.is_file())
        _record("A: size_rotations == 2", w.stats.size_rotations == 2)
        _record(
            "A: active base holds only the post-002 tail",
            base.read_text(encoding="utf-8") == "post-002 tail\n",
        )
        if seg1.is_file():
            with gzip.open(seg1, "rt", encoding="utf-8") as fh:
                seg1_lines = fh.read().count("\n")
            _record("A: segment 001 gzip preserved 8 lines", seg1_lines == 8)
        else:
            _record("A: segment 001 gzip preserved 8 lines", False)
        _record("A: no *.gz.tmp partial remains", list(d.glob("*.gz.tmp")) == [])

    # (B) (date, seq) prune ordering / keep-newest, plus direct sort-key pin.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # Seed daily + numbered segments OUT of lexical order on disk, each an
        # exact 1000-byte ".gz" (raw bytes; the prune path never decompresses).
        seeded = [
            "syslog.log.2026-06-02.001.gz",  # newest (survivor)
            "syslog.log.2026-06-01.002.gz",
            "syslog.log.2026-06-01.gz",  # bare daily: (date, -1), oldest in its day
            "syslog.log.2026-06-01.001.gz",
        ]
        for name in seeded:
            (d / name).write_bytes(b"\x00" * 1000)
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        # total fixed, free huge (floor never trips); cap = 10% of 10_000 = 1000
        # bytes admits exactly one segment, so the loop prunes oldest-first until
        # only the highest-(date, seq) segment survives.
        vol = _FakeVolume(total=10_000, free=10_000)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,  # retention must not prune the seeded segments
            now=clock,
            max_log_percent=10,  # cap = 1000 bytes -> exactly one segment fits
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        w.enforce_space_tick()
        w.close()
        victims = _prune_victims(warn)
        survivors = sorted(p.name for p in d.glob("syslog.log.*.gz"))
        keys_sorted = victims == sorted(victims, key=segment_sort_key)
        _record("B: victims pruned in ascending (date, seq) order", keys_sorted)
        _record(
            "B: bare daily (date,-1) pruned before same-day 001",
            "syslog.log.2026-06-01.gz" in victims
            and (
                victims.index("syslog.log.2026-06-01.gz")
                < victims.index("syslog.log.2026-06-01.001.gz")
            ),
        )
        _record(
            "B: highest (date, seq) segment survives",
            survivors == ["syslog.log.2026-06-02.001.gz"],
        )
        # Direct sort-key pin on a hand-built name list (independent of pruning).
        hand = [
            "syslog.log.2026-06-01.002.gz",
            "syslog.log.2026-06-01.gz",
            "syslog.log.2026-06-02.001.gz",
            "syslog.log.2026-06-01.001.gz",
        ]
        expect = [
            "syslog.log.2026-06-01.gz",  # (2026-06-01, -1)
            "syslog.log.2026-06-01.001.gz",
            "syslog.log.2026-06-01.002.gz",
            "syslog.log.2026-06-02.001.gz",
        ]
        _record(
            "B: segment_sort_key orders daily<001<002<next-day",
            sorted(hand, key=segment_sort_key) == expect,
        )

    # (C) both-constraint prune: the floor dimension is driven via the injected
    # volume_stats seam (free climbs as segments are pruned), and the loop exits
    # the moment BOTH predicates hold — not before, not after.
    class _SteppingVolume:
        """A volume whose free space climbs one step per *consumed* read.

        Models the reality that each prune frees space: every call returns the
        current free, then advances it by ``step`` for the next iteration, so the
        guard's re-measure-each-iteration loop observes free rising toward the
        floor. ``total`` is fixed for deterministic percentage budgets.
        """

        def __init__(self, total: int, free: int, step: int) -> None:
            self.total = total
            self.free = free
            self.step = step

        def __call__(self, _path: Path) -> tuple[int, int]:
            current = self.free
            self.free += self.step
            return (self.total, current)

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # Five exact-100-byte ".gz" files (raw bytes; the prune path never
        # decompresses, it only stat()s + unlink()s). Both floor and cap active.
        # Basis total=1000: floor=30% -> free must reach 300; cap=40% -> log_dir
        # must fall to <=400 bytes (i.e. <=4 segments). Floor binds harder here.
        for i in range(1, 6):
            (d / f"syslog.log.2026-06-01.{i:03d}.gz").write_bytes(b"\x00" * 100)
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        # free starts 240 (<300 floor) and climbs +20 per read; the pre-loop read
        # consumes 240->260, then each loop-top read advances. Floor (free>=300)
        # is reached after enough reads that two prunes have occurred.
        stepping = _SteppingVolume(total=1000, free=240, step=20)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            min_free_percent=30,  # floor: free >= 300
            max_log_percent=40,  # cap: log_dir <= 400 bytes (<= 4 segments)
            max_segment_mb=1,
            warn=warn,
            volume_stats=stepping,
        )
        before_c = 5
        w.enforce_space_tick()
        w.close()
        after_c = len(list(d.glob("syslog.log.*.gz")))
        pruned_c = before_c - after_c
        # It cannot exit before the floor is met (free starts below floor and only
        # the prune loop's re-reads raise it), so at least one prune must occur.
        _record("C: both-constraint loop pruned at least one segment", pruned_c >= 1)
        # And it must NOT over-prune to zero: once both predicates hold it stops,
        # so survivors remain (both limits are satisfiable here).
        _record(
            "C: both-constraint loop stops once both hold (survivors remain)",
            after_c >= 1,
        )

    # (C-cap) cap-only: pruning stops as soon as log_dir <= cap (no over-prune).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # Four exact-100-byte segments (400 bytes of logs). Floor disabled (free
        # 100%); cap basis total=500 -> cap=60% = 300 bytes -> holds 3 segments,
        # so exactly one is pruned and three survive (no over-prune).
        for i in range(1, 5):
            (d / f"syslog.log.2026-06-01.{i:03d}.gz").write_bytes(b"\x00" * 100)
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        vol = _FakeVolume(total=500, free=500)  # free 100% -> floor never trips
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_log_percent=60,  # cap = 300 bytes -> 3 segments fit
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        w.enforce_space_tick()
        w.close()
        survivors_cap = len(list(d.glob("syslog.log.*.gz")))
        _record(
            "C-cap: cap-only stops as soon as log_dir <= cap (3 survive)",
            survivors_cap == 3,
        )

    # (D) restart-safe next-seq: a second Writer in the same dir continues seq.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=30, now=clock, max_segment_mb=1)
        for _ in range(16):  # two rolls -> 001, 002
            w.write(_LINE_128K)
        w.close()
        d = Path(tmp)
        had_002 = (d / "syslog.log.2026-06-01.002.gz").is_file()
        # Second Writer, same dir + same UTC day: next roll must be 003, no clobber.
        clock2 = _FakeClock(day0)
        w2 = Writer(tmp, "syslog.log", retention_days=30, now=clock2, max_segment_mb=1)
        for _ in range(8):  # one more roll
            w2.write(_LINE_128K)
        w2.close()
        had_003 = (d / "syslog.log.2026-06-01.003.gz").is_file()
        still_002 = (d / "syslog.log.2026-06-01.002.gz").is_file()
        _record("D: first Writer produced 002", had_002)
        _record("D: restart continues at next free seq (003)", had_003)
        _record("D: prior 002 segment not clobbered", still_002)

    # (E) ring-buffer keep-newest end-to-end: tight cap, many segments, newest K
    # survive; active base intact; counters reflect deletions.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        seeded_e = [f"syslog.log.2026-06-01.{i:03d}.gz" for i in range(1, 7)]
        for name in seeded_e:  # exact 2000-byte raw ".gz" files
            (d / name).write_bytes(b"\x00" * 2000)
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        vol = _FakeVolume(total=1_000_000, free=999_999)  # floor never trips
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_log_percent=1,  # cap = 10_000 bytes -> only newest few survive
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        w.write("active stays\n")
        before_e = len(seeded_e)
        w.enforce_space_tick()
        base_text = (d / "syslog.log").read_text(encoding="utf-8")
        w.close()
        survivors_e = sorted(p.name for p in d.glob("syslog.log.*.gz"))
        # Survivors must be a newest-suffix of the seeded ascending list.
        survived_count = len(survivors_e)
        keep_newest = (
            survived_count < before_e
            and survivors_e == seeded_e[before_e - survived_count :]
        )
        _record("E: ring buffer kept the newest K segments", keep_newest)
        _record("E: active base intact after pruning", base_text == "active stays\n")
        _record(
            "E: space_prunes counts the deletions",
            w.stats.space_prunes == before_e - survived_count,
        )
        _record("E: bytes_reclaimed > 0", w.stats.bytes_reclaimed > 0)

    # (F) terminal: only active file left, still over budget.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # One prunable segment; cap so tight it can never be satisfied by pruning.
        with gzip.open(
            d / "syslog.log.2026-06-01.001.gz", "wt", encoding="utf-8"
        ) as fh:
            fh.write("w" * 4096 + "\n")
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        vol = _FakeVolume(total=1_000_000, free=1)  # free ~0% << any floor
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            min_free_percent=50,  # floor unsatisfiable by pruning logs
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        w.write("active line\n")
        w.enforce_space_tick()  # must terminate, not loop forever
        base_text = (d / "syslog.log").read_text(encoding="utf-8")
        w.close()
        exhausted = [k for k, _ in warn.calls if k == "space:exhausted"]
        _record("F: guard terminates (no infinite loop)", True)
        _record("F: active base not deleted/truncated", base_text == "active line\n")
        _record("F: exactly one space:exhausted WARNING", len(exhausted) == 1)
        _record(
            "F: space_prunes counts only the removable segment",
            w.stats.space_prunes == 1,
        )

    # (G) statvfs failure degrades safe.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        with gzip.open(
            d / "syslog.log.2026-06-01.001.gz", "wt", encoding="utf-8"
        ) as fh:
            fh.write("g" * 1024 + "\n")
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        raising_vol = _RaisingVolume()
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_log_percent=1,
            max_segment_mb=1,
            warn=warn,
            volume_stats=raising_vol,
        )
        w.enforce_space_tick()  # statvfs raises -> warn, no prune
        statvfs_warns = [k for k, _ in warn.calls if k == "space:statvfs"]
        seg_present = (d / "syslog.log.2026-06-01.001.gz").is_file()
        # Subsequent write() still works (write path does not touch volume_stats
        # when no size-roll occurs; here the active file is well under 1 MiB).
        wrote_ok = True
        try:
            w.write("after statvfs failure\n")
        except WriteError:
            wrote_ok = False
        w.close()
        _record("G: one space:statvfs WARNING", len(statvfs_warns) == 1)
        _record("G: no prune occurred (segment intact)", seg_present)
        _record("G: no space_prunes counted", w.stats.space_prunes == 0)
        _record("G: subsequent write() works normally", wrote_ok)

    # (H) seq overflow terminal: pre-seed 999, drive next-seq > 999.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        with gzip.open(
            d / "syslog.log.2026-06-01.999.gz", "wt", encoding="utf-8"
        ) as fh:
            fh.write("seeded 999\n")
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_segment_mb=1,
            warn=warn,
        )
        for _ in range(8):  # cross 1 MiB -> next seq would be 1000
            w.write(_LINE_128K)
        # The base is now over-threshold; one more write proves it keeps
        # accepting past the seq-overflow terminal (the un-throttled recording
        # stub records each over-threshold write's warn; the live server throttles
        # them to one per window).
        w.write("still accepting\n")
        w.close()
        overflow_keys = {k for k, _ in warn.calls if k == "space:seq-overflow"}
        overflow_count = len([k for k, _ in warn.calls if k == "space:seq-overflow"])
        no_1000 = not (d / "syslog.log.2026-06-01.1000.gz").exists()
        base_text = (d / "syslog.log").read_text(encoding="utf-8")
        _record(
            "H: seq-overflow WARNING fired (single throttle key)",
            overflow_count >= 1 and overflow_keys == {"space:seq-overflow"},
        )
        _record("H: no segment beyond 999 produced", no_1000)
        _record(
            "H: size_rotations stayed 0 (roll skipped)", w.stats.size_rotations == 0
        )
        _record(
            "H: active file keeps accepting writes",
            base_text.endswith("still accepting\n"),
        )

    # (I) guard disabled == 1.2.0: all three knobs 0 -> only daily archives.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=30, now=clock)  # all knobs 0
        for _ in range(16):  # would roll twice IF size-rotation were enabled
            w.write(_LINE_128K)
        clock.value = day0 + timedelta(days=1)
        w.write("day1\n")
        w.close()
        d = Path(tmp)
        numbered = list(d.glob("syslog.log.*.[0-9][0-9][0-9].gz"))
        daily = d / "syslog.log.2026-06-01.gz"
        _record("I: no numbered segments produced when disabled", numbered == [])
        _record("I: daily archive still produced", daily.is_file())
        _record("I: size_rotations == 0 when disabled", w.stats.size_rotations == 0)
        _record("I: space_prunes == 0 when disabled", w.stats.space_prunes == 0)

    # (J) reconcile of NUMBERED orphans (the size-segment trio): three states
    # seeded at once for a prior UTC day, mirroring the daily-form Case (4) but
    # for the <date>.<NNN> forms the size guard introduced.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        prior = (day0 - timedelta(days=1)).date().isoformat()
        # (a) uncompressed numbered orphan, NO final -> re-gzipped, source gone.
        orphan_a = d / f"syslog.log.{prior}.001"
        orphan_a.write_text("orphan 001 body\n", encoding="utf-8")
        # (b) uncompressed numbered orphan WITH an existing final .gz (crash after
        #     replace, before unlink) -> redundant source removed, final kept.
        orphan_b_src = d / f"syslog.log.{prior}.002"
        orphan_b_src.write_text("dup 002 source\n", encoding="utf-8")
        orphan_b_final = d / f"syslog.log.{prior}.002.gz"
        with gzip.open(orphan_b_final, "wt", encoding="utf-8") as fh:
            fh.write("002 final already\n")
        # (c) stale numbered *.gz.tmp partial -> deleted, none remain.
        partial_c = d / f"syslog.log.{prior}.003.gz.tmp"
        partial_c.write_bytes(b"partial 003 garbage")

        clock = _FakeClock(day0)
        Writer(tmp, "syslog.log", retention_days=30, now=clock).close()

        regz_a = d / f"syslog.log.{prior}.001.gz"
        _record(
            "J: numbered uncompressed orphan re-gzipped, source removed",
            regz_a.is_file() and not orphan_a.exists(),
        )
        if regz_a.is_file():
            with gzip.open(regz_a, "rt", encoding="utf-8") as fh:
                regz_a_body = fh.read()
            _record(
                "J: re-gzipped numbered orphan preserves its body",
                regz_a_body == "orphan 001 body\n",
            )
        else:
            _record("J: re-gzipped numbered orphan preserves its body", False)
        if orphan_b_final.is_file():
            with gzip.open(orphan_b_final, "rt", encoding="utf-8") as fh:
                orphan_b_body = fh.read()
        else:
            orphan_b_body = ""
        _record(
            "J: numbered dup source removed, existing final kept untouched",
            orphan_b_final.is_file()
            and not orphan_b_src.exists()
            and orphan_b_body == "002 final already\n",
        )
        _record(
            "J: numbered *.gz.tmp partial deleted, none remain",
            not partial_c.exists() and list(d.glob("*.gz.tmp")) == [],
        )

    # (K) next-seq scans AFTER reconcile: a same-UTC-day uncompressed orphan
    # (005) is re-gzipped at construction, so the FIRST live size-roll must land
    # on 006 (proving _next_seq reads the post-reconcile segment set, not a
    # pre-reconcile snapshot that would collide at 001).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        today = day0.date().isoformat()
        orphan_005 = d / f"syslog.log.{today}.005"
        orphan_005.write_text("pre-existing 005\n", encoding="utf-8")
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=30, now=clock, max_segment_mb=1)
        regz_005 = d / f"syslog.log.{today}.005.gz"
        for _ in range(8):  # cross 1 MiB exactly once -> one live roll
            w.write(_LINE_128K)
        w.close()
        seg_006 = d / f"syslog.log.{today}.006.gz"
        clobber_001 = d / f"syslog.log.{today}.001.gz"
        _record(
            "K: same-day orphan re-gzipped to 005 at construction", regz_005.is_file()
        )
        _record(
            "K: first live roll lands on 006 (next-seq is post-reconcile)",
            seg_006.is_file(),
        )
        _record("K: roll did not collide back at 001", not clobber_001.exists())

    # (M1) in-loop measurement failure -> space:scandir, degrade-safe. The volume
    # seam succeeds on the pre-loop read (so _enforce_space enters the prune loop)
    # then raises on the next read (the loop-top re-measure), exercising the
    # in-loop except branch distinct from the pre-loop space:statvfs branch.
    class _FailAfterVolume:
        """Succeeds for the first ``ok_calls`` reads, then raises ``OSError``."""

        def __init__(self, total: int, free: int, ok_calls: int) -> None:
            self.total = total
            self.free = free
            self.ok_calls = ok_calls
            self.calls = 0

        def __call__(self, _path: Path) -> tuple[int, int]:
            self.calls += 1
            if self.calls > self.ok_calls:
                raise OSError("injected in-loop measurement failure")
            return (self.total, self.free)

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        with gzip.open(
            d / "syslog.log.2026-06-01.001.gz", "wt", encoding="utf-8"
        ) as fh:
            fh.write("m1" * 1024 + "\n")
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        # Pre-loop read (call 1) returns free 0% -> floor unmet -> enter loop;
        # the loop-top re-measure (call 2) raises -> space:scandir, no prune.
        fail_after = _FailAfterVolume(total=1_000_000, free=1, ok_calls=1)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            min_free_percent=50,
            max_segment_mb=1,
            warn=warn,
            volume_stats=fail_after,
        )
        w.enforce_space_tick()
        scandir_warns = [k for k, _ in warn.calls if k == "space:scandir"]
        statvfs_warns_m1 = [k for k, _ in warn.calls if k == "space:statvfs"]
        seg_intact = (d / "syslog.log.2026-06-01.001.gz").is_file()
        wrote_ok_m1 = True
        try:
            w.write("after scandir failure\n")
        except WriteError:
            wrote_ok_m1 = False
        w.close()
        _record("M1: exactly one space:scandir WARNING", len(scandir_warns) == 1)
        _record(
            "M1: pre-loop space:statvfs did NOT fire (read 1 succeeded)",
            statvfs_warns_m1 == [],
        )
        _record("M1: no prune occurred (segment intact)", seg_intact)
        _record("M1: no space_prunes counted", w.stats.space_prunes == 0)
        _record("M1: subsequent write() works normally", wrote_ok_m1)

    # (M2) unlink failure mid-prune -> space:unlink, degrade-safe. With no inject
    # seam for unlink, the parent dir is made non-writable (0o500) so victim
    # .unlink() raises PermissionError (an OSError); permissions are restored in
    # finally so the TemporaryDirectory can clean up.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "syslog.log.2026-06-01.001.gz").write_bytes(b"\x00" * 1000)
        base_m2 = d / "syslog.log"
        base_m2.write_text("active m2\n", encoding="utf-8")
        clock = _FakeClock(day0)
        warn = RecordingWarn()
        # cap so tight the lone segment is over budget -> guard tries to prune it.
        vol = _FakeVolume(total=10_000, free=10_000)  # floor disabled (free 100%)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_log_percent=1,  # cap = 100 bytes; the 1000-byte segment is over
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        os.chmod(d, 0o500)  # read+execute, no write -> unlink raises
        try:
            w.enforce_space_tick()  # must warn, not crash
        finally:
            os.chmod(d, 0o700)  # restore for assertions + cleanup
        unlink_warns = [k for k, _ in warn.calls if k == "space:unlink"]
        seg_still = (d / "syslog.log.2026-06-01.001.gz").is_file()
        base_still = base_m2.read_text(encoding="utf-8")
        w.close()
        _record("M2: exactly one space:unlink WARNING", len(unlink_warns) == 1)
        _record("M2: no crash; loop returned after the failed unlink", True)
        _record(
            "M2: space_prunes NOT counted on a failed unlink", w.stats.space_prunes == 0
        )
        _record("M2: un-unlinkable victim segment still present", seg_still)
        _record("M2: active base untouched", base_still == "active m2\n")

    # (M3 / M4) the live gauges Writer.disk_free_pct / log_dir_mb feed the stats
    # line. M3 pins the happy figures + the two None paths; M4 pins that the
    # log-dir byte sum counts ONLY regular files (skips a subdir + a symlink).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # Two known-size regular .gz files: 3 MiB + 1 MiB = 4 MiB of logs.
        (d / "syslog.log.2026-06-01.001.gz").write_bytes(b"\x00" * (3 * 1024 * 1024))
        (d / "syslog.log.2026-06-01.002.gz").write_bytes(b"\x00" * (1 * 1024 * 1024))
        clock = _FakeClock(day0)
        vol = _FakeVolume(total=1000, free=250)  # 25% free
        w = Writer(tmp, "syslog.log", retention_days=3650, now=clock, volume_stats=vol)
        # Writer.__init__ opens an (empty) active base; it is a regular file but
        # contributes 0 bytes, so the whole-MB figure stays the segments' 4 MiB.
        _record("M3: disk_free_pct == free*100//total (25)", w.disk_free_pct() == 25)
        _record(
            "M3: log_dir_mb sums regular files to whole MB (4)", w.log_dir_mb() == 4
        )

        # M4: a subdirectory and a symlink-to-large-file must NOT count toward
        # the log-dir byte sum (is_file(follow_symlinks=False) skips both).
        (d / "subdir").mkdir()
        (d / "subdir" / "big.gz").write_bytes(b"\x00" * (5 * 1024 * 1024))
        large_target = d / "target_large.bin"
        large_target.write_bytes(b"\x00" * (9 * 1024 * 1024))
        link = d / "syslog.log.2026-06-01.003.gz"
        link.symlink_to(large_target)
        _record(
            "M4: log_dir_mb ignores subdir + symlink (still 4 + target_large)",
            # target_large.bin is itself a regular file at the top level (9 MiB),
            # so the regular-file sum is now 4 + 9 == 13 MiB; the 5 MiB subdir
            # file and the symlink (which points at the same 9 MiB) add nothing.
            w.log_dir_mb() == 13,
        )
        w.close()

    # (M3-None) the two None gauge paths: a raising volume seam, and a degenerate
    # total<=0 volume. Both must render the stats line as '?' (return None) rather
    # than crash. Separate Writers so each sees only its own volume seam.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            volume_stats=_RaisingVolume(),
        )
        _record(
            "M3: disk_free_pct() is None on measurement OSError",
            w.disk_free_pct() is None,
        )
        w.close()
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            volume_stats=_FakeVolume(total=0, free=0),
        )
        _record(
            "M3: disk_free_pct() is None when total <= 0", w.disk_free_pct() is None
        )
        w.close()

    # (L2) a single oversized line (~1.5 MiB) lands in the active base, and the
    # NEXT write triggers exactly one roll whose segment holds the full oversized
    # line intact (the active file may exceed the threshold by one datagram).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=3650, now=clock, max_segment_mb=1)
        big_line = "z" * (3 * 512 * 1024 - 1) + "\n"  # ~1.5 MiB, single line
        w.write(big_line)  # over 1 MiB after this write -> rolls on this call
        seg_l2 = d / "syslog.log.2026-06-01.001.gz"
        _record("L2: oversized single line triggers one roll", seg_l2.is_file())
        _record("L2: exactly one size_rotation", w.stats.size_rotations == 1)
        if seg_l2.is_file():
            with gzip.open(seg_l2, "rt", encoding="utf-8") as fh:
                seg_l2_body = fh.read()
            _record(
                "L2: rolled segment holds the full oversized line",
                seg_l2_body == big_line,
            )
        else:
            _record("L2: rolled segment holds the full oversized line", False)
        w.close()

    # (L3) segment gzip content is byte-exact: the rolled .gz decompresses to the
    # exact concatenation of the lines written into it (Case A checks line COUNT;
    # this pins the bytes, catching a future truncation/interleave regression).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=3650, now=clock, max_segment_mb=1)
        written_lines = [f"L3 line {i:04d}\n" for i in range(8)]
        body = "".join(written_lines)
        # Pad to just over 1 MiB so exactly one roll captures these 8 lines plus
        # a trailing filler line; assert the segment holds the 8 lines verbatim.
        w.write(body)
        w.write(_LINE_128K * 8)  # push the active file past 1 MiB -> one roll
        seg_l3 = d / "syslog.log.2026-06-01.001.gz"
        if seg_l3.is_file():
            with gzip.open(seg_l3, "rt", encoding="utf-8") as fh:
                seg_l3_body = fh.read()
            _record(
                "L3: rolled segment begins with the exact written lines",
                seg_l3_body.startswith(body),
            )
        else:
            _record("L3: rolled segment begins with the exact written lines", False)
        w.close()

    ok = all(passed for _, passed in results)
    print(f"STORAGE CHECK {'PASSED' if ok else 'FAILED'}", file=sys.stderr)
    return ok
