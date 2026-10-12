"""§15.3 row on the counts-only allowlist (plan :1108 and row 1398).

For each variable, with stale output folders already under ``--out``, the
counts-only header's key set equals the 17 allowlisted keys exactly and
holds no candidate value, ``failed_dirs``, ``owner_answers`` or
``not_built``; the freeze-prepare outputs (rain's header, temperature's
draft note) list ``failed_dirs`` relative to ``--out``, with a
``.partial/`` folder listed under its ``.failed/`` name.
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
from typing import Literal, cast

import pytest

from tests.helpers import shadow_build_precip_copy, shadow_set_copy_mtime
from wxverify.verification import daily_shadow

COMMIT_SHA = "a" * 40

ALLOWLIST: frozenset[str] = frozenset(
    {
        "copy",
        "manifest",
        "commit_sha",
        "gates",
        "arm",
        "counts",
        "unmatched",
        "stale_truth_rows",
        "station_set",
        "station_set_changed",
        "db_import",
        "db_import_changed",
        "excluded_h_dates_count",
        "h_training_coverage",
        "rain_event_counts",
        "meteoblue",
        "coverage",
    }
)

FORBIDDEN_KEYS = frozenset({"failed_dirs", "owner_answers", "not_built"})

# Keys that would carry a value: the generic value fields and every
# candidate name the temperature and rain lines use.
CANDIDATE_VALUE_KEYS = frozenset(
    {
        "value",
        "amount",
        "S-Q",
        "CH-Q",
        "R-clim",
        "R0",
        "MB-R0",
        "R1",
        *daily_shadow.TEMPERATURE_CANDIDATES,
    }
)


def _stub_check_commit(package_dir: Path, *, commit_sha: str) -> dict[str, object]:
    del commit_sha
    return daily_shadow.package_manifest(package_dir)


def _run_main(
    *,
    copy: Path,
    out: Path,
    site_id: int,
    variable: Literal["temperature", "precip"],
    mode: Literal["counts-only", "freeze-prepare"],
    monkeypatch: pytest.MonkeyPatch,
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
        variable,
        "--commit-sha",
        COMMIT_SHA,
    ]
    match mode:
        case "counts-only":
            argv.append("--counts-only")
        case "freeze-prepare":
            argv.extend(["--freeze-prepare", "--house-station", "SYN-HOUSE"])
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = daily_shadow.main(argv)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def _run_dir(out: Path, out_text: str) -> Path:
    lines = [
        line
        for line in out_text.splitlines()
        if line.startswith(daily_shadow.RESULT_PREFIX)
    ]
    assert len(lines) == 1
    payload = json.loads(lines[0][len(daily_shadow.RESULT_PREFIX) :])
    return out / cast(str, payload["run_dir"])


def _all_keys(value: object) -> set[str]:
    """Every dict key found at any depth under ``value``."""
    keys: set[str] = set()
    if isinstance(value, dict):
        mapping = cast("dict[str, object]", value)
        keys.update(mapping.keys())
        for nested in mapping.values():
            keys.update(_all_keys(nested))
    elif isinstance(value, list):
        for entry in cast("list[object]", value):
            keys.update(_all_keys(entry))
    return keys


def _prepare_out(tmp_path: Path) -> Path:
    out = tmp_path / "out"
    out.mkdir()
    (out / "old.failed").mkdir()
    (out / "x.partial").mkdir()
    return out


@pytest.mark.parametrize("variable", ["temperature", "precip"])
def test_counts_only_header_holds_exactly_the_allowlisted_keys(
    variable: Literal["temperature", "precip"],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A counts-only header's key set is exactly the 17 allowlisted keys.

    No key naming a candidate or a value field (``CANDIDATE_VALUE_KEYS``,
    or any key containing ``candidate``) and none of ``failed_dirs``,
    ``owner_answers`` or ``not_built`` appear at any depth, for either
    variable.
    """
    copy, site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)
    out = _prepare_out(tmp_path)

    exit_code, _out_text, err_text = _run_main(
        copy=copy,
        out=out,
        site_id=site.site_id,
        variable=variable,
        mode="counts-only",
        monkeypatch=monkeypatch,
    )

    assert exit_code == 0, err_text
    assert "renamed an unfinished output to x.failed" in err_text
    run_dir = _run_dir(out, _out_text)
    assert {p.name for p in run_dir.iterdir()} == {"header.json"}
    header = json.loads((run_dir / "header.json").read_text())

    assert len(ALLOWLIST) == 17
    assert set(header) == ALLOWLIST
    assert _all_keys(header).isdisjoint(FORBIDDEN_KEYS)
    assert _all_keys(header).isdisjoint(CANDIDATE_VALUE_KEYS)
    assert not any("candidate" in key for key in _all_keys(header))


@pytest.mark.parametrize("variable", ["temperature", "precip"])
def test_freeze_prepare_lists_failed_dirs_relative_to_out(
    variable: Literal["temperature", "precip"],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``failed_dirs`` names stale output folders relative to ``--out``.

    Rain writes ``failed_dirs`` in its header, temperature in its draft
    note; the names are relative to ``--out``, and the renamed
    ``.partial/`` folder appears under its ``.failed/`` name.
    """
    copy, site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)
    out = _prepare_out(tmp_path)

    exit_code, out_text, err_text = _run_main(
        copy=copy,
        out=out,
        site_id=site.site_id,
        variable=variable,
        mode="freeze-prepare",
        monkeypatch=monkeypatch,
    )

    assert exit_code == 0, err_text
    assert "renamed an unfinished output to x.failed" in err_text
    assert not (out / "x.partial").exists()
    assert (out / "x.failed").is_dir()

    run_dir = _run_dir(out, out_text)
    if variable == "precip":
        header = json.loads((run_dir / "header.json").read_text())
        assert header["failed_dirs"] == ["old.failed", "x.failed"]
    else:
        draft = json.loads((run_dir / "freeze_note.draft.json").read_text())
        assert draft["failed_dirs"] == ["old.failed", "x.failed"]


def test_paired_positive_freeze_prepare_header_holds_keys_outside_the_allowlist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-counts-only header carries keys the allowlist check would reject.

    Shows the counts-only test above can fail on this fixture: a header
    that is not counts-only does carry other keys, including
    ``failed_dirs``.
    """
    copy, site = shadow_build_precip_copy(tmp_path, name="copy.db")
    shadow_set_copy_mtime(copy)
    out = _prepare_out(tmp_path)

    exit_code, out_text, err_text = _run_main(
        copy=copy,
        out=out,
        site_id=site.site_id,
        variable="precip",
        mode="freeze-prepare",
        monkeypatch=monkeypatch,
    )

    assert exit_code == 0, err_text
    run_dir = _run_dir(out, out_text)
    header = json.loads((run_dir / "header.json").read_text())
    assert not set(header).issubset(ALLOWLIST)
    assert "failed_dirs" in header
