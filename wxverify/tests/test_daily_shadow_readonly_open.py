"""§15.3 "Read-only open": the copy is opened through a URI that refuses writes.

Pins two layers of ``open_copy_readonly`` (``daily_shadow.py:642-661``): the
``PRAGMA query_only=ON`` the function itself sets, and the ``mode=ro`` URI
the connection is opened with underneath it. Test 2 turns ``query_only``
back off to show the two layers are independent -- test 1 alone cannot
tell a copy that is read-only only because of the pragma apart from one
that is read-only because of the URI as well. Also pins that a full run
leaves the copy's bytes and mtime untouched, with a paired positive
(test 4) showing an ordinary write through the same file would move both,
since the fixture's mtime is a fixed 2037 date that no unrelated process
would otherwise disturb.
"""

from __future__ import annotations

import contextlib
import io
import sqlite3
from pathlib import Path

import pytest

from tests.helpers import shadow_build_precip_copy, shadow_set_copy_mtime
from wxverify.verification import daily_shadow

COMMIT_SHA = "b" * 40


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _run_main(
    *, copy: Path, out: Path, site_id: int, monkeypatch: pytest.MonkeyPatch
) -> tuple[int, str, str]:
    monkeypatch.setattr(daily_shadow, "check_commit", _stub_check_commit)
    monkeypatch.setenv("SQLITE_TMPDIR", str(copy.parent))
    argv = [
        "--copy",
        str(copy),
        "--site-id",
        str(site_id),
        "--out",
        str(out),
        "--variable",
        "precip",
        "--commit-sha",
        COMMIT_SHA,
        "--freeze-prepare",
        "--house-station",
        "SYN-HOUSE",
    ]
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def test_open_copy_readonly_reads_query_only_as_1_and_rejects_an_insert(
    tmp_path: Path,
) -> None:
    copy, _site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)

    conn = daily_shadow.open_copy_readonly(copy)
    try:
        row = conn.execute("PRAGMA query_only").fetchone()
        assert row is not None
        assert int(row[0]) == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute(
                "INSERT INTO settings (key, value) VALUES ('syn-key', 'syn-value')"
            )
    finally:
        conn.close()


def test_the_read_only_uri_still_rejects_an_insert_with_query_only_off(
    tmp_path: Path,
) -> None:
    """``query_only=OFF`` alone does not open the copy to writes.

    Test 1 by itself cannot tell whether the rejection comes from the
    ``PRAGMA query_only=ON`` the function sets, or from the ``mode=ro``
    URI the connection is opened with underneath it -- a copy opened
    without ``?mode=ro`` would accept the insert once ``query_only`` is
    turned off. This test turns the pragma off and shows the insert still
    fails, so the ``mode=ro`` layer is pinned on its own.
    """
    copy, _site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)

    conn = daily_shadow.open_copy_readonly(copy)
    try:
        conn.execute("PRAGMA query_only=OFF")
        row = conn.execute("PRAGMA query_only").fetchone()
        assert row is not None
        assert int(row[0]) == 0
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute(
                "INSERT INTO settings (key, value) VALUES ('syn-key', 'syn-value')"
            )
    finally:
        conn.close()


def test_a_full_run_leaves_the_copy_sha256_and_mtime_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    copy, site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)
    before_sha = daily_shadow.sha256_file(copy)
    before_mtime = copy.stat().st_mtime_ns

    out = tmp_path / "out"
    exit_code, out_text, err_text = _run_main(
        copy=copy, out=out, site_id=site.site_id, monkeypatch=monkeypatch
    )

    assert exit_code == 0, err_text
    prefix = daily_shadow.RESULT_PREFIX
    out_lines = [line for line in out_text.splitlines() if line.startswith(prefix)]
    assert len(out_lines) == 1
    assert daily_shadow.sha256_file(copy) == before_sha
    assert copy.stat().st_mtime_ns == before_mtime


def test_paired_positive_a_write_changes_the_copy_sha256_and_mtime(
    tmp_path: Path,
) -> None:
    """Shows the previous test's checks can fail on this fixture.

    The fixture's mtime is pinned to a fixed 2037 date, so any real write
    through the file moves both its sha256 and its mtime -- an ordinary
    write, not the tool under test, is enough to flip the checks above.
    """
    copy, _site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)
    before_sha = daily_shadow.sha256_file(copy)
    before_mtime = copy.stat().st_mtime_ns

    conn = sqlite3.connect(copy)
    try:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES ('syn-key', 'syn-value')"
        )
        conn.commit()
    finally:
        conn.close()

    assert daily_shadow.sha256_file(copy) != before_sha
    assert copy.stat().st_mtime_ns != before_mtime
