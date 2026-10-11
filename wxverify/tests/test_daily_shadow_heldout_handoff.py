"""``main()``'s ``--heldout`` argument handoff to ``run()`` (plan :1076,
:1079, :805).

Plan :1076: "``--heldout`` (temperature only; refused with ``--variable
precip``): targets from the note's first H date (§10.2) on, less the
note's ``excluded_h_dates``."

Plan :1079: "The bounds are required keyword arguments of the tool's run
function. ``main()`` passes the module constants ``CHOOSING_FIRST`` and
``CHOOSING_LAST``, the note's first H date, or, for rain, S0 and the
copy's last complete local day, and passes the note's
``excluded_h_dates`` to the case selector as a required keyword argument
(an empty list when the note has none, and for rain)."

Plan :805: "``excluded_h_dates``: the sorted list (ISO dates) of H dates
on which a candidate value was read live (§7.1), empty if there are
none. ``main()`` parses it and passes it to the case selector as a
required keyword argument; it removes those dates as case targets
only, never as training days;"

Each test builds the same real, migrated copy (``_make_copy``) holding
one site (time zone UTC, from ``asof_make_site``), with its mtime set
to a fixed snapshot of 2031-02-10T02:00:00Z. The site's time zone is
UTC, so ``latest_knowable_target`` compares local-day boundaries
directly against that UTC snapshot with no offset. With
``CONSENSUS_LAG_HOURS = 3``, stepping back from the snapshot's local
date 2031-02-10:

- 2031-02-10: end 2031-02-11T00:00Z + 3 h = 2031-02-11T03:00Z, after
  the snapshot, so step back;
- 2031-02-09: end 2031-02-10T00:00Z + 3 h = 2031-02-10T03:00Z, after
  the snapshot, so step back;
- 2031-02-08: end 2031-02-09T00:00Z + 3 h = 2031-02-09T03:00Z, not
  after the snapshot, so 2031-02-08 is the last complete day.

This date is asserted as the literal 2031-02-08, never read back from
``latest_knowable_target`` itself.

Test (a) drives ``--heldout`` with an on-disk freeze note whose
``first_h_date`` is 2031-01-01 and whose ``excluded_h_dates`` are
2031-01-05 and 2031-01-09. Test (b) drives ``--freeze-prepare`` on a
copy of the same shape and snapshot, with no note. ``main()`` refuses
``--heldout`` without a note and ``--house-station`` outside
``--freeze-prepare``, so the pair also differs in the mode and in
those mode-required flags; it is the closest pair ``main()`` accepts.
The handoff fields that depend on the note (``h_first``, the recorded
``note``, ``excluded_h_dates``) differ between the two accordingly,
while ``last_complete`` -- derived from the copy's mtime, not the note
-- is identical in both.

No mutant row claims this file's kills; it covers a handoff these plan
excerpts describe and that no §15.3-named test targets. Four
supplemental probes -- each a deliberate change to ``main()`` that a
test here must catch -- with the assertion that catches each:

- h_first always ``DEFAULT_H_FIRST`` -> test
  ``test_heldout_note_overrides_h_first_and_excluded_dates`` at
  ``h_first``: correct = 2031-01-01, probe = 2026-09-08.
- ``excluded_h_dates`` always ``[]`` -> test
  ``test_heldout_note_overrides_h_first_and_excluded_dates`` at
  ``excluded_h_dates``: correct = [2031-01-05, 2031-01-09], probe = [].
- ``note=None`` passed to ``run()`` regardless of ``--heldout`` -> test
  ``test_heldout_note_overrides_h_first_and_excluded_dates`` at the
  note's type check.
- ``last_complete`` taken as the copy's mtime date rather than the
  knowable-target date -> tests
  ``test_heldout_note_overrides_h_first_and_excluded_dates`` and
  ``test_freeze_prepare_with_no_note_uses_defaults`` at
  ``last_complete``: correct = 2031-02-08, probe = 2031-02-10.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import sqlite3
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from tests.helpers import asof_make_site
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import daily_shadow

COMMIT_SHA = "b" * 40
SNAPSHOT_UTC = datetime(2031, 2, 10, 2, 0, 0, tzinfo=UTC)
LAST_COMPLETE_DAY = date(2031, 2, 8)


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _make_copy(tmp_path: Path) -> tuple[Path, int]:
    """A real, migrated copy holding one site, frozen at ``SNAPSHOT_UTC``."""
    copy = tmp_path / "copy.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "heldout-handoff-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    timestamp = SNAPSHOT_UTC.timestamp()
    os.utime(copy, (timestamp, timestamp))
    return copy, site_id


def _recorder(
    calls: list[dict[str, object]],
) -> Callable[..., dict[str, bytes]]:
    def _run(conn: sqlite3.Connection, **kwargs: object) -> dict[str, bytes]:
        del conn
        calls.append(kwargs)
        return {"header.json": b"{}"}

    return _run


def _invoke_main(
    *,
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    copy: Path,
    calls: list[dict[str, object]],
) -> tuple[int, str]:
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setattr(daily_shadow, "run", _recorder(calls))
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue() + stderr.getvalue()


def test_heldout_note_overrides_h_first_and_excluded_dates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy, site_id = _make_copy(tmp_path)
    expect_sha256 = hashlib.sha256(copy.read_bytes()).hexdigest()

    note_payload: dict[str, object] = {
        "format": daily_shadow.NOTE_FORMAT,
        "site": {"id": site_id},
        "first_h_date": "2031-01-01",
        "excluded_h_dates": ["2031-01-05", "2031-01-09"],
        "station_set": [],
        "db_import": None,
        "manifest": daily_shadow.package_manifest(daily_shadow.package_root()),
        "constants": daily_shadow.frozen_constants(),
        "house_station": "SYN-HOUSE",
        "td_k": daily_shadow.TD_K_DEFAULT,
    }
    note_path = tmp_path / "note.json"
    note_path.write_text(json.dumps(note_payload), encoding="utf-8")

    calls: list[dict[str, object]] = []
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(tmp_path / "out"),
        "--variable",
        "temperature",
        "--commit-sha",
        COMMIT_SHA,
        "--heldout",
        "--freeze-note",
        str(note_path),
        "--expect-sha256",
        expect_sha256,
    ]
    exit_code, text = _invoke_main(
        argv=argv, monkeypatch=monkeypatch, copy=copy, calls=calls
    )

    assert exit_code == 0, text
    assert len(calls) == 1
    kwargs = calls[0]

    assert kwargs["mode"] == "heldout"
    assert kwargs["variable"] == "temperature"
    assert kwargs["first"] == date(2026, 7, 25)
    assert kwargs["last"] == date(2026, 9, 7)
    assert kwargs["h_first"] == date(2031, 1, 1)
    assert kwargs["excluded_h_dates"] == [date(2031, 1, 5), date(2031, 1, 9)]
    assert kwargs["last_complete"] == LAST_COMPLETE_DAY
    assert kwargs["t2_last"] is None

    note = kwargs["note"]
    assert isinstance(note, daily_shadow.FreezeNote)
    assert note.site_id == site_id
    assert note.first_h_date == date(2031, 1, 1)
    assert note.excluded_h_dates == (date(2031, 1, 5), date(2031, 1, 9))
    assert note.house_station == "SYN-HOUSE"
    assert note.td_k == daily_shadow.TD_K_DEFAULT


def test_freeze_prepare_with_no_note_uses_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy, site_id = _make_copy(tmp_path)

    calls: list[dict[str, object]] = []
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(tmp_path / "out"),
        "--variable",
        "temperature",
        "--commit-sha",
        COMMIT_SHA,
        "--freeze-prepare",
        "--house-station",
        "SYN-HOUSE",
    ]
    exit_code, text = _invoke_main(
        argv=argv, monkeypatch=monkeypatch, copy=copy, calls=calls
    )

    assert exit_code == 0, text
    assert len(calls) == 1
    kwargs = calls[0]

    assert kwargs["mode"] == "freeze-prepare"
    assert kwargs["first"] == date(2026, 7, 25)
    assert kwargs["last"] == date(2026, 9, 7)
    assert kwargs["h_first"] == date(2026, 9, 8)
    assert kwargs["excluded_h_dates"] == []
    assert kwargs["note"] is None
    assert kwargs["last_complete"] == LAST_COMPLETE_DAY
    assert kwargs["t2_last"] is None
