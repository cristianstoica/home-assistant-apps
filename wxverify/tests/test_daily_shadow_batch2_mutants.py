"""§15.3/§15.4 batch 2: ``copy_damaged``/``find_sqlite_errorcode`` pins, the
``main()`` error wiring (ERROR, §15.3 :1324/:1327), freeze enforcement
(M16, M24 and the :1336 argparse case) and Arm-selection gates M13/M13b
(§15.3 :1312; §15.4).

M25 (F1 on_time-only), M26/M79 (date bounds), M27 (lower bound, both
arms), M29 (excluded H dates), M30 (station set) and M31 (stale truth)
are NOT covered in this file.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from tests.helpers import asof_make_site
from tests.test_daily_shadow_output_folder import (
    _run_main,  # pyright: ignore[reportPrivateUsage]
)
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import daily_shadow

COMMIT_SHA = "a" * 40


def _real_sha256(path: Path) -> str:
    """The file's sha256, computed independently of
    ``daily_shadow.sha256_file`` (F6): a test must not use the function
    under test to build its own oracle for it, or a mutant that makes
    ``sha256_file`` return a constant would pass every before/after and
    ``--expect-sha256`` check in this file trivially."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


# --- Task 1: ``copy_damaged`` is pure (§15.3 :1326; kills M14) -------------


def test_m14_copy_damaged_uses_the_low_byte_not_the_full_code() -> None:
    """M14: ``copy_damaged`` compares the full code against ``{11, 26}``
    instead of its low byte (``code & 0xFF``) -> at
    ``copy_damaged(267)``: correct = True (267 & 0xFF == 11, SQLITE_CORRUPT),
    mutant = False (267 not in {11, 26})."""
    assert daily_shadow.copy_damaged(267) is True
    assert daily_shadow.copy_damaged(11) is True
    assert daily_shadow.copy_damaged(26) is True


def test_copy_damaged_paired_positive_undamaged_codes_and_none() -> None:
    """Paired positive/support: a code whose low byte is not a damaged
    primary code is False; no code at all is None (unclassified, not
    "undamaged") -- ``is`` used throughout so a falsy-but-wrong type can't
    slip past ``==``."""
    assert daily_shadow.copy_damaged(522) is False  # 522 & 0xFF == 10
    assert daily_shadow.copy_damaged(5) is False
    assert daily_shadow.copy_damaged(None) is None


# --- Task 2: ``find_sqlite_errorcode`` pin (§15.3 :1325) -------------------


def _locked_error(tmp_path: Path) -> sqlite3.OperationalError:
    """A genuine ``sqlite3.OperationalError`` with a real ``sqlite_errorcode``.

    Produced by real lock contention (one connection holds an exclusive
    write transaction while a second, zero-timeout connection tries to
    write), not by hand-setting the attribute -- the code (5, SQLITE_BUSY)
    is whatever SQLite itself assigns.
    """
    path = tmp_path / "locked.db"
    with contextlib.closing(sqlite3.connect(path)) as holder:
        holder.execute("CREATE TABLE t (x)")
        holder.commit()
        holder.execute("BEGIN EXCLUSIVE")
        holder.execute("INSERT INTO t VALUES (1)")
        with contextlib.closing(sqlite3.connect(path, timeout=0)) as contender:
            try:
                contender.execute("INSERT INTO t VALUES (2)")
            except sqlite3.OperationalError as exc:
                return exc
    raise AssertionError("the locked-database fixture did not raise")


def test_find_sqlite_errorcode_a_codeless_error_hides_the_code_behind_it(
    tmp_path: Path,
) -> None:
    """§15.3 :1325 pin: a codeless ``sqlite3.Error`` found first (via
    ``__cause__``) hides a coded ``sqlite3.Error`` further along the chain --
    the function stops at the FIRST ``sqlite3.Error``, whatever it carries.
    """
    locked = _locked_error(tmp_path)
    assert locked.sqlite_errorcode == 5
    try:
        try:
            raise locked
        except sqlite3.OperationalError as cause:
            raise sqlite3.Error("outer, codeless") from cause
    except sqlite3.Error as exc:
        result = daily_shadow.find_sqlite_errorcode(exc)
    assert result is None


def test_find_sqlite_errorcode_paired_positive_first_error_carries_a_code(
    tmp_path: Path,
) -> None:
    """Paired positive: with nothing codeless in front of it, the same
    locked-database error's own code (5) is returned."""
    locked = _locked_error(tmp_path)
    result = daily_shadow.find_sqlite_errorcode(locked)
    assert result == 5


# --- Tasks 3/4: the ``main()`` error wiring (§15.3 :1324, :1327) -----------


def _chained_267() -> BaseException:
    inner = sqlite3.OperationalError("disk I/O error")
    inner.sqlite_errorcode = 267
    try:
        raise inner
    except sqlite3.OperationalError as cause:
        try:
            raise RuntimeError("load_site forced failure (chained)") from cause
        except RuntimeError as exc:
            return exc


def _direct_code_5() -> BaseException:
    err = sqlite3.OperationalError("database is locked")
    err.sqlite_errorcode = 5
    return err


def _no_sqlite_cause() -> BaseException:
    return RuntimeError("load_site forced failure (no SQLite cause)")


def _run_dir_entries(out: Path, run_dir: str) -> list[str]:
    root = out / run_dir
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


@pytest.mark.parametrize(
    ("make_exc", "expected_code", "classification"),
    [
        (
            _chained_267,
            267,
            "; copy damaged (SQLite code 267)",
        ),
        (
            _direct_code_5,
            5,
            "; SQLite code 5 does not mark the copy damaged;"
            " that is not a check of the copy",
        ),
        (
            _no_sqlite_cause,
            None,
            "; no SQLite code; copy damage not classified",
        ),
    ],
    ids=["chained-267-damaged", "direct-5-not-damaged", "no-sqlite-cause"],
)
def test_main_error_wiring_classifies_and_records_each_forced_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_exc: Any,
    expected_code: int | None,
    classification: str,
) -> None:
    """§15.3 ERROR + the wiring through ``main()`` (:1324, :1327): three
    forced failures at ``load_site`` -- the seam ``main()`` calls right
    after opening the copy, never the error handler itself.

    M14 is killed by the stderr suffix "; copy damaged (SQLite code 267)"
    for the chained-267 case, not by ``error.json["sqlite_errorcode"]`` --
    that field pins the separate rule that the code is recorded unmasked.
    """
    copy = _make_output_folder_copy(tmp_path)
    before_sha = _real_sha256(copy)
    out = tmp_path / "out"

    forced_exc = make_exc()

    def forced_load_site(
        conn: sqlite3.Connection, *, site_id: int
    ) -> daily_shadow.Site:
        del conn, site_id
        raise forced_exc

    monkeypatch.setattr(daily_shadow, "load_site", forced_load_site)
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch
    )

    assert exit_code == 1
    assert open_calls == 1

    expected_text = f"{type(forced_exc).__name__}: {forced_exc}"
    expected_stderr = f"ERROR at stage run: {expected_text}{classification}"
    assert err_text.strip() == expected_stderr

    result_lines = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(result_lines) == 1
    payload = json.loads(result_lines[0][len(daily_shadow.RESULT_PREFIX) :])
    assert payload["exit_code"] == 1
    run_dir = payload["run_dir"]
    assert run_dir is not None

    error_path = out / run_dir / "error.json"
    error = json.loads(error_path.read_text(encoding="utf-8"))
    assert set(error) == {
        "stage",
        "error",
        "sqlite_errorcode",
        "copy_sha256",
        "manifest",
    }
    assert error["stage"] == "run"
    assert error["error"] == expected_text
    assert error["sqlite_errorcode"] == expected_code
    assert error["copy_sha256"] == before_sha

    assert _real_sha256(copy) == before_sha
    assert _run_dir_entries(out, run_dir) == ["error.json"]
    assert daily_shadow.list_failed_dirs(out) == []
    # ``run_dir`` is a POSIX-form relative path with several components
    # (date/variable/mode-timestamp, per ``main()``'s ``final_dir``), so a
    # single ``out.iterdir()`` only sees its first segment -- walk every
    # segment and require it to be the ONLY entry at that level, so a
    # mutant leaving a sibling ``.partial``/``.failed`` directory anywhere
    # along the path, or a stray file directly under ``out``, is caught.
    current = out
    for part in Path(run_dir).parts:
        assert sorted(p.name for p in current.iterdir()) == [part]
        current = current / part


def _make_output_folder_copy(tmp_path: Path) -> Path:
    """A real, migrated copy holding one site. Mirrors
    ``test_daily_shadow_output_folder._make_copy``; kept local so the
    fixture's site name is distinct."""
    copy = tmp_path / "copy.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "batch2-wiring-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    return copy


# --- M16/M24: freeze enforcement (§15.3 :1336) -----------------------------


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _flip_one_hex_digit(digest: str) -> str:
    table = {"0": "1", "1": "2", "a": "b", "f": "0"}
    last = digest[-1]
    flipped = table.get(last, "0" if last != "0" else "1")
    assert flipped != last
    return digest[:-1] + flipped


def _freeze_note(
    *, manifest: dict[str, object], constants: dict[str, object]
) -> daily_shadow.FreezeNote:
    return daily_shadow.FreezeNote(
        site_id=1,
        first_h_date=date(2031, 1, 1),
        excluded_h_dates=(),
        station_set=(),
        db_import=None,
        manifest=manifest,
        constants=constants,
        house_station="SYN-HOUSE",
        td_k=daily_shadow.TD_K_DEFAULT,
    )


def _real_manifest() -> dict[str, object]:
    return daily_shadow.package_manifest(daily_shadow.package_root())


def _with_one_file_digit_flipped(manifest: dict[str, object]) -> dict[str, object]:
    raw_files = manifest["files"]
    assert isinstance(raw_files, list)
    typed_files = cast("list[list[str]]", raw_files)
    files = [list(entry) for entry in typed_files]
    assert files, "the real package manifest has no .py files to flip"
    path, digest = files[0]
    files[0] = [path, _flip_one_hex_digit(digest)]
    return {**manifest, "files": files}


def test_m16a_check_note_matches_package_files_differ() -> None:
    """M16a: the files comparison (``note.manifest.get("files") !=
    manifest.get("files")``) deleted from ``check_note_matches_package`` ->
    at the raised ``RefusedError`` when one file's recorded sha256 differs
    by a single hex digit (python version held equal): correct =
    RefusedError("the package manifest differs from the freeze note's"),
    mutant = no exception (the files comparison never runs, so a changed
    file goes unnoticed)."""
    manifest = _real_manifest()
    note = _freeze_note(
        manifest=_with_one_file_digit_flipped(manifest),
        constants=daily_shadow.frozen_constants(),
    )
    with pytest.raises(
        daily_shadow.RefusedError,
        match=r"^the package manifest differs from the freeze note's$",
    ):
        daily_shadow.check_note_matches_package(note, manifest=manifest)


def test_m16a_check_note_matches_package_python_version_differs() -> None:
    """Support case on the real manifest shape (``python``: ``[major,
    minor]``): a differing Python version is refused by its own, earlier
    check, before the files comparison is ever reached."""
    manifest = _real_manifest()
    raw_python = manifest["python"]
    assert isinstance(raw_python, list)
    python = cast("list[int]", raw_python)
    changed = {**manifest, "python": [python[0], python[1] + 1]}
    note = _freeze_note(manifest=changed, constants=daily_shadow.frozen_constants())
    with pytest.raises(
        daily_shadow.RefusedError,
        match=r"^the Python version differs from the freeze note's$",
    ):
        daily_shadow.check_note_matches_package(note, manifest=manifest)


def test_m16a_paired_positive_matching_manifest_and_constants_passes() -> None:
    """Paired positive: the real, unmodified manifest and the real
    ``frozen_constants()`` raise nothing -- the two cases above are genuine
    mismatches, not an artifact of ``check_note_matches_package`` always
    refusing."""
    manifest = _real_manifest()
    note = _freeze_note(manifest=manifest, constants=daily_shadow.frozen_constants())
    daily_shadow.check_note_matches_package(note, manifest=manifest)


def _run_main_heldout_with_note(
    *,
    copy: Path,
    out: Path,
    note: daily_shadow.FreezeNote | None,
    expect_sha256: str,
    site_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[int, str, str, int]:
    """Like ``_run_main_heldout``, but ``daily_shadow.load_freeze_note`` is
    monkeypatched to hand back ``note`` directly -- the seam ``main()``
    calls, so no on-disk note file or its JSON schema is involved. A
    sibling of ``_run_main`` (``test_daily_shadow_output_folder``), which
    hard-codes ``--freeze-prepare``/``--house-station`` and so cannot drive
    ``--heldout``.
    """
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    if note is not None:
        fixed_note = note

        def _load_note(path: Path) -> daily_shadow.FreezeNote:
            del path
            return fixed_note

        monkeypatch.setattr(daily_shadow, "load_freeze_note", _load_note)
    real_open = daily_shadow.open_copy_readonly
    open_calls = 0

    def open_recorder(path: Path) -> object:
        nonlocal open_calls
        open_calls += 1
        return real_open(path)

    monkeypatch.setattr(daily_shadow, "open_copy_readonly", open_recorder)
    note_path = copy.parent / "note.json"
    note_path.write_text("{}", encoding="utf-8")
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out),
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
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue(), open_calls


def test_m16b_main_refuses_a_heldout_run_whose_note_manifest_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M16b: the only call to ``check_note_matches_package`` in ``main()``'s
    ``--heldout`` path deleted -> at ``main()``'s stderr and open count with
    a note whose manifest has one file's sha256 flipped (``--expect-sha256``
    correct, isolating the manifest check): correct = exit 2,
    "refused: the package manifest differs from the freeze note's", 0
    opens; mutant = the check never runs, so the refusal is absent and the
    run proceeds to open the copy.
    """
    copy = _make_output_folder_copy(tmp_path)
    before_sha = _real_sha256(copy)
    manifest = _real_manifest()
    note = _freeze_note(
        manifest=_with_one_file_digit_flipped(manifest),
        constants=daily_shadow.frozen_constants(),
    )
    out = tmp_path / "out"

    exit_code, out_text, err_text, open_calls = _run_main_heldout_with_note(
        copy=copy,
        out=out,
        note=note,
        expect_sha256=before_sha,
        site_id=1,
        monkeypatch=monkeypatch,
    )

    assert exit_code == 2
    assert err_text.strip() == (
        "refused: the package manifest differs from the freeze note's"
    )
    assert daily_shadow.RESULT_PREFIX not in out_text
    assert open_calls == 0
    assert _real_sha256(copy) == before_sha


def test_m16b_paired_positive_main_does_not_refuse_a_matching_note(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive: the identical setup with the UNCHANGED real
    manifest in the note never hits the "manifest differs" refusal and
    proceeds to open the copy -- proving the negative case above is a
    genuine mismatch, not main() refusing regardless of the note."""
    copy = _make_output_folder_copy(tmp_path)
    before_sha = _real_sha256(copy)
    manifest = _real_manifest()
    note = _freeze_note(manifest=manifest, constants=daily_shadow.frozen_constants())
    out = tmp_path / "out"

    exit_code, out_text, err_text, open_calls = _run_main_heldout_with_note(
        copy=copy,
        out=out,
        note=note,
        expect_sha256=before_sha,
        site_id=1,
        monkeypatch=monkeypatch,
    )

    assert "manifest differs" not in err_text
    assert open_calls == 1
    del exit_code, out_text


def _open_recorder(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Counts calls to ``open_copy_readonly`` the same way
    ``_run_main_heldout_with_note`` (above) and ``_run_main_heldout``
    (below) do, for a test that drives ``main()`` directly rather than
    through ``_run_main_heldout*``."""
    real_open = daily_shadow.open_copy_readonly
    calls = [0]

    def open_recorder(path: Path) -> object:
        calls[0] += 1
        return real_open(path)

    monkeypatch.setattr(daily_shadow, "open_copy_readonly", open_recorder)
    return calls


def test_freeze_enforcement_heldout_without_freeze_note_is_an_argparse_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """§15.3 :1336's argparse case (freeze enforcement). No row of the
    mutant table targets this check; M16 (manifest check skipped, :1423)
    is targeted by the M16a/M16b tests above.

    ``--heldout`` without ``--freeze-note`` exits with an argparse error
    (``main()``'s own ``parser.error(...)`` at :6666-6667) -> at the
    exit code and stderr: correct = ``SystemExit(2)`` with "--heldout
    needs --freeze-note and --expect-sha256" on stderr and zero database
    opens.

    If :6666-6667 were deleted, ``main()`` would go on to ``check_commit``
    (not stubbed here), which refuses because the package's real HEAD is
    not ``"a" * 40`` (:964-968). ``main()`` would then return 2 with
    "refused: commit check: ..." on stderr instead of raising
    ``SystemExit``, so ``pytest.raises(SystemExit)`` fails (DID NOT
    RAISE), and the message assertion is a second, independent kill. The
    matching case with both flags present gets past this check and opens
    the copy (``open_calls == 1``) in
    ``test_m16b_paired_positive_main_does_not_refuse_a_matching_note``.
    """
    calls = _open_recorder(monkeypatch)
    argv = [
        "--copy",
        str(tmp_path / "copy.db"),
        "--site-id",
        "1",
        "--out",
        str(tmp_path / "out"),
        "--variable",
        "temperature",
        "--commit-sha",
        COMMIT_SHA,
        "--heldout",
        "--expect-sha256",
        "a" * 64,
    ]
    with pytest.raises(SystemExit) as excinfo:
        daily_shadow.main(argv)
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert captured.err.startswith("usage:")
    assert "--heldout needs --freeze-note and --expect-sha256" in captured.err
    assert calls[0] == 0


def test_freeze_enforcement_heldout_without_expect_sha256_is_an_argparse_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """§15.3 :1336's argparse case (freeze enforcement). No row of the
    mutant table targets this check; M16 (manifest check skipped, :1423)
    is targeted by the M16a/M16b tests above.

    ``--heldout`` without ``--expect-sha256`` exits with the same
    argparse error -> at the exit code and stderr: correct =
    ``SystemExit(2)`` with "--heldout needs --freeze-note and
    --expect-sha256" on stderr and zero database opens.

    If :6666-6667 were deleted, the next check this run would reach is
    ``load_freeze_note(note_path)`` (:6676): the given ``--freeze-note``
    path does not exist, so reading it raises an ``OSError`` that
    ``load_freeze_note`` itself catches and re-raises as
    ``RefusedError(f"freeze note unreadable: {exc}")`` (:1315-1320).
    ``main()`` returns 2 instead of raising ``SystemExit``, so
    ``pytest.raises(SystemExit)`` fails (DID NOT RAISE), and the message
    assertion is a second, independent kill. The matching case with both
    flags present gets past this check and opens the copy
    (``open_calls == 1``) in
    ``test_m16b_paired_positive_main_does_not_refuse_a_matching_note``.
    """
    calls = _open_recorder(monkeypatch)
    argv = [
        "--copy",
        str(tmp_path / "copy.db"),
        "--site-id",
        "1",
        "--out",
        str(tmp_path / "out"),
        "--variable",
        "temperature",
        "--commit-sha",
        COMMIT_SHA,
        "--heldout",
        "--freeze-note",
        str(tmp_path / "note.json"),
    ]
    with pytest.raises(SystemExit) as excinfo:
        daily_shadow.main(argv)
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert captured.err.startswith("usage:")
    assert "--heldout needs --freeze-note and --expect-sha256" in captured.err
    assert calls[0] == 0


def _make_heldout_note(*, site_id: int) -> dict[str, object]:
    manifest = daily_shadow.package_manifest(daily_shadow.package_root())
    constants = daily_shadow.frozen_constants()
    return {
        "format": daily_shadow.NOTE_FORMAT,
        "site": {"id": site_id},
        "first_h_date": "2031-01-01",
        "excluded_h_dates": [],
        "station_set": [],
        "db_import": None,
        "manifest": manifest,
        "constants": constants,
        "house_station": "SYN-HOUSE",
        "td_k": daily_shadow.TD_K_DEFAULT,
    }


def _run_main_heldout(
    *,
    copy: Path,
    out: Path,
    note_path: Path,
    expect_sha256: str,
    site_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[int, str, str, int]:
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    real_open = daily_shadow.open_copy_readonly
    open_calls = 0

    def open_recorder(path: Path) -> object:
        nonlocal open_calls
        open_calls += 1
        return real_open(path)

    monkeypatch.setattr(daily_shadow, "open_copy_readonly", open_recorder)
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out),
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
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue(), open_calls


def test_m24_expect_sha256_off_by_one_hex_digit_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M24: the ``--expect-sha256`` comparison skipped -> at the
    refusal text printed on stderr when ``before != expect``: correct =
    "refused: the copy's sha256 differs from --expect-sha256" (the run
    never opens the copy), mutant = no such refusal (the string is simply
    absent, since a mutant that always "passes" the check prints nothing
    here and proceeds to open the copy instead).
    """
    copy = _make_output_folder_copy(tmp_path)
    before_sha = _real_sha256(copy)
    wrong_sha = _flip_one_hex_digit(before_sha)
    assert wrong_sha != before_sha
    assert len(wrong_sha) == len(before_sha)

    note_path = tmp_path / "note.json"
    note_path.write_text(json.dumps(_make_heldout_note(site_id=1)), encoding="utf-8")
    out = tmp_path / "out"

    exit_code, out_text, err_text, open_calls = _run_main_heldout(
        copy=copy,
        out=out,
        note_path=note_path,
        expect_sha256=wrong_sha,
        site_id=1,
        monkeypatch=monkeypatch,
    )

    assert exit_code == 2
    assert err_text.strip() == (
        "refused: the copy's sha256 differs from --expect-sha256"
    )
    assert daily_shadow.RESULT_PREFIX not in out_text
    assert open_calls == 0
    assert _real_sha256(copy) == before_sha


def test_m24_paired_positive_matching_expect_sha256_is_not_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive: the identical setup with the CORRECT sha256 never
    hits the "differs from --expect-sha256" refusal and proceeds to open
    the copy -- proving the negative case above is a genuine mismatch, not
    an artifact of some other refusal firing first."""
    copy = _make_output_folder_copy(tmp_path)
    before_sha = _real_sha256(copy)

    note_path = tmp_path / "note.json"
    note_path.write_text(json.dumps(_make_heldout_note(site_id=1)), encoding="utf-8")
    out = tmp_path / "out"

    exit_code, out_text, err_text, open_calls = _run_main_heldout(
        copy=copy,
        out=out,
        note_path=note_path,
        expect_sha256=before_sha,
        site_id=1,
        monkeypatch=monkeypatch,
    )

    assert "differs from --expect-sha256" not in err_text
    assert open_calls == 1
    del exit_code, out_text


# --- Task 5: arm selection, M13 and M13b (§15.3 :1312; §15.4) --------------


def _site(rain_threshold_mm: float = 0.2) -> daily_shadow.Site:
    return daily_shadow.Site(
        site_id=1,
        name="arm-selection-site",
        enabled=True,
        elevation_m=100.0,
        rain_threshold_mm=rain_threshold_mm,
        generation_id=1,
        timezone="UTC",
    )


def test_m13_no_in_scope_readings_fails_ga_and_selects_arm_b() -> None:
    """M13: G-a's ``readings > 0`` dropped, leaving only
    ``null_fetched == 0`` -> at ``GateReport.arm`` with zero readings on
    every in-scope day (and every published row matching the resulting
    "no data" rebuild outcome, so G-b passes identically either way):
    correct = "B" (``ga_pass`` False: ``readings == 0`` fails the dropped
    clause), mutant = "A" (``null_fetched == 0`` holds vacuously on zero
    readings, so ``ga_pass`` is wrongly True).
    """
    site = _site()
    days = [date(2031, 1, 1) + timedelta(days=i) for i in range(5)]
    quantities = daily_shadow.TEMPERATURE_QUANTITIES
    published = {
        (quantity, day): daily_shadow.PublishedTruth(
            value=None, eligible=False, stale=False, rain_threshold_mm=None
        )
        for day in days
        for quantity in quantities
    }
    rebuilder = daily_shadow.ArmARebuilder([], site=site, variable="temperature")
    report = daily_shadow.compute_gates(
        rebuilder,
        published,
        in_scope_days=days,
        quantities=quantities,
        site=site,
        force_arm=None,
    )
    assert report.ga_readings == 0
    assert report.ga_null_fetched == 0
    assert report.ga_pass is False
    assert report.gb_denominator == len(days)
    assert report.gb_matched == len(days)
    assert report.gb_pass is True
    assert report.arm == "B"


def test_m13_paired_positive_in_scope_readings_pass_ga() -> None:
    """Paired positive: one known (non-null ``fetched_at``) reading per
    in-scope day makes ``readings > 0`` true, so G-a passes -- showing the
    negative case above is a genuine zero, not a fixture that can never
    pass G-a at all."""
    site = _site()
    days = [date(2031, 1, 1) + timedelta(days=i) for i in range(5)]
    rows = [
        daily_shadow.StationRow(
            pws_station_id=f"SYN-{i}",
            station_id=i + 1,
            dem_elevation_m=100.0,
            valid_at=f"{day.isoformat()}T12:00:00+00:00",
            local_date=day,
            value=20.0,
            fetched_at=datetime(day.year, day.month, day.day, 6, tzinfo=UTC),
        )
        for i, day in enumerate(days)
    ]
    rebuilder = daily_shadow.ArmARebuilder(rows, site=site, variable="temperature")
    report = daily_shadow.compute_gates(
        rebuilder,
        {},
        in_scope_days=days,
        quantities=daily_shadow.TEMPERATURE_QUANTITIES,
        site=site,
        force_arm=None,
    )
    assert report.ga_readings == len(days)
    assert report.ga_null_fetched == 0
    assert report.ga_pass is True


def test_m13b_every_in_scope_day_stale_fails_gb_and_selects_arm_b() -> None:
    """M13b: G-b's ``denominator > 0`` dropped, leaving only the
    percentage comparison -> at ``GateReport.arm`` with every in-scope
    day's published rows stale (so no day reaches the denominator, and
    G-a passes identically either way via real, non-null readings):
    correct = "B" (``gb_pass`` False: ``denominator == 0`` fails the
    dropped clause), mutant = "A" (``100 * 0 >= GB_MIN_PERCENT * 0`` holds
    vacuously, so ``gb_pass`` is wrongly True).
    """
    site = _site()
    days = [date(2031, 2, 1) + timedelta(days=i) for i in range(4)]
    quantities = daily_shadow.TEMPERATURE_QUANTITIES
    rows = [
        daily_shadow.StationRow(
            pws_station_id=f"SYN-{i}",
            station_id=i + 1,
            dem_elevation_m=100.0,
            valid_at=f"{day.isoformat()}T12:00:00+00:00",
            local_date=day,
            value=20.0,
            fetched_at=datetime(day.year, day.month, day.day, 6, tzinfo=UTC),
        )
        for i, day in enumerate(days)
    ]
    published: dict[tuple[str, date], daily_shadow.PublishedTruth] = {}
    for day in days:
        published[("temperature_high", day)] = daily_shadow.PublishedTruth(
            value=20.0, eligible=True, stale=True, rain_threshold_mm=None
        )
        published[("temperature_low", day)] = daily_shadow.PublishedTruth(
            value=10.0, eligible=True, stale=False, rain_threshold_mm=None
        )
    rebuilder = daily_shadow.ArmARebuilder(rows, site=site, variable="temperature")
    report = daily_shadow.compute_gates(
        rebuilder,
        published,
        in_scope_days=days,
        quantities=quantities,
        site=site,
        force_arm=None,
    )
    assert report.ga_readings == len(days)
    assert report.ga_pass is True
    assert report.gb_denominator == 0
    assert report.gb_stale_days == len(days)
    assert report.gb_pass is False
    assert report.arm == "B"


def test_m13b_paired_positive_one_non_stale_day_gives_a_positive_denominator() -> None:
    """Paired positive: with one in-scope day's truth rows both non-stale,
    G-b's denominator is 1, not 0 -- the all-stale case above is a genuine
    0/0, not an artifact of the fixture."""
    site = _site()
    days = [date(2031, 3, 1) + timedelta(days=i) for i in range(4)]
    published: dict[tuple[str, date], daily_shadow.PublishedTruth] = {}
    for i, day in enumerate(days):
        stale = i != 0
        published[("temperature_high", day)] = daily_shadow.PublishedTruth(
            value=20.0, eligible=True, stale=stale, rain_threshold_mm=None
        )
        published[("temperature_low", day)] = daily_shadow.PublishedTruth(
            value=10.0, eligible=True, stale=False, rain_threshold_mm=None
        )
    rebuilder = daily_shadow.ArmARebuilder([], site=site, variable="temperature")
    report = daily_shadow.compute_gates(
        rebuilder,
        published,
        in_scope_days=days,
        quantities=daily_shadow.TEMPERATURE_QUANTITIES,
        site=site,
        force_arm=None,
    )
    assert report.gb_denominator == 1
    assert report.gb_stale_days == 3
