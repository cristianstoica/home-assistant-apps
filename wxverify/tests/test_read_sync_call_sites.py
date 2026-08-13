"""Guards ``Database.read_sync``'s one sanctioned call site.

``read_sync`` is the CLI's synchronous escape hatch: unlike ``read``, it is
never gated against an in-progress import swap, never drawn from the bounded
read pool (so it is never drained across a swap either), and never counted
in read-timing instrumentation. That is fine for the CLI, which runs before
or outside any event loop -- but a route handler or worker job that started
calling it would acquire all three defects silently, with nothing at the
call site itself to fail loudly. This test makes that property a build-time
check instead of a convention nobody enforces.
"""

from __future__ import annotations

from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "wxverify"
_DEFINITION_SITE = "wxverify/db/connection.py"
_ALLOWED_CALLERS = frozenset({"wxverify/__main__.py"})
#: Raised 5 -> 6 for `wxverify timezone status`, a genuine new read-only CLI
#: call site (§20 operator surface).
_EXPECTED_TOTAL_CALLS = 6


def test_read_sync_is_only_called_from_the_cli_entry_point() -> None:
    callers: dict[str, int] = {}
    for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
        rel = path.relative_to(_PACKAGE_ROOT.parent).as_posix()
        if rel == _DEFINITION_SITE:
            continue
        count = path.read_text(encoding="utf-8").count("read_sync(")
        if count:
            callers[rel] = count

    unexpected = set(callers) - _ALLOWED_CALLERS
    assert not unexpected, (
        f"read_sync() must only be called from {sorted(_ALLOWED_CALLERS)}, "
        f"but it is also called from {sorted(unexpected)}. read_sync() "
        "bypasses the import-swap gate, the bounded read pool, and "
        "read-timing instrumentation -- the new caller must instead take "
        "the write lock around the call, or switch to `await read()`. Do "
        "not add the new file to an exemption list here."
    )

    total_calls = sum(callers.values())
    assert total_calls == _EXPECTED_TOTAL_CALLS, (
        f"expected exactly {_EXPECTED_TOTAL_CALLS} read_sync() call sites "
        f"in {sorted(_ALLOWED_CALLERS)}, found {total_calls}. If this is a "
        "genuine new CLI call site, update _EXPECTED_TOTAL_CALLS above; "
        "otherwise take the write lock around the call, or switch to "
        "`await read()`."
    )
