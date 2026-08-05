"""Tests for the 0.8.9 `.bak` retention sweep + `backup_exclude`
static config content.

Harness idioms are copied from ``tests/test_db_transfer.py`` (``_init_tmp_db``,
``_make_app``/``_idle_worker``, ``_csrf_headers``, ``_build_replacement_db``,
the ``TestClient`` inline-``BackgroundTask`` idiom) rather than reinventing
them, per this repo's one-file-per-feature-slice convention (no cross-file
imports between test modules exist in this repo; helpers are duplicated
verbatim as the established idiom, matching ``test_write_lock_serialization.py``
duplicating ``test_db_transfer.py``'s ``_init_tmp_db``).

All fixture `.bak`/DB files are built via ``_make_valid_bak``: a minimal but
gate-passing synthetic SQLite database (``PRAGMA user_version = 42`` plus
empty ``sites``/``stations``/``station_observations`` tables) -- never a copy
of a real `wxverify.db`. A bare ``CREATE TABLE t(x)`` database will not do:
its ``user_version`` is 0 and it has none of ``_REQUIRED_TABLES``, so
``_looks_like_valid_sqlite`` rejects it, the sweep skips pruning entirely (the
"corrupt newest" abort path), and a retention test built on it would fail for
a reason unrelated to sort order. All embedded backup timestamps use the
synthetic year 2000 (sequential seconds), never a real-looking backup
schedule.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from starlette.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.routes import db_transfer
from wxverify.api.routes.db_transfer import _looks_like_valid_sqlite, sweep_bak_files
from wxverify.db.connection import Database, close_db, init_db

# ---------------------------------------------------------------------------
# Harness (idiom copied from test_db_transfer.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    """Drop-in run_worker shim that idles without touching the scheduler."""
    import asyncio

    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites
            (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
        VALUES (?, 47.0, 25.0, 900.0, 'UTC', 1)
        """,
        (name,),
    )
    site_id = cur.lastrowid
    assert site_id is not None, "INSERT must produce a rowid"
    return site_id


def _csrf_headers(
    client: TestClient, *, origin: str = "http://testserver"
) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {
        "Origin": origin,
        "X-CSRF-Token": token,
        "Content-Type": "application/octet-stream",
    }


def _build_replacement_db(tmp_path: Path, filename: str, site_name: str) -> Path:
    """Build a standalone, fully-migrated DB file seeded with one site."""
    path = tmp_path / filename
    db = Database(str(path))
    try:
        _make_site(db._conn, site_name)  # noqa: SLF001
        db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001
    finally:
        db.close()
    return path


# --- .bak fixture builders ---------------------------------------------------


# Synthetic, non-real-looking embedded timestamps: fixed year 2000, sequential
# seconds -- never a date that could be read as a real backup schedule.
def _ts(seq: int) -> str:
    return f"2000010100000{seq}"[:8] + "-" + f"2000010100000{seq}"[8:14]


def _bak_name(ts: str) -> str:
    return f"wxverify-{ts}Z.db.bak"


def _make_valid_bak(path: Path, *, user_version: int = 42) -> Path:
    """Minimal synthetic SQLite file that passes ``_looks_like_valid_sqlite``."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(f"PRAGMA user_version = {user_version}")
        conn.execute("CREATE TABLE sites(x)")
        conn.execute("CREATE TABLE stations(x)")
        conn.execute("CREATE TABLE station_observations(x)")
        conn.commit()
    finally:
        conn.close()
    return path


def _seed_bak(db_dir: Path, seq: int) -> Path:
    """Create a valid `.bak` at synthetic sequence position ``seq``."""
    return _make_valid_bak(db_dir / _bak_name(_ts(seq)))


def _existing_baks(db_dir: Path) -> list[str]:
    return sorted(p.name for p in db_dir.glob("wxverify-*.db.bak"))


# ---------------------------------------------------------------------------
# Unit-level sweep tests (direct calls, no HTTP/app).
# ---------------------------------------------------------------------------


def test_sweep_keeps_only_newest(tmp_path: Path) -> None:
    """4 distinct-timestamped valid `.bak` files -> only the newest survives.

    Catches: an inverted sort (oldest kept instead of newest), or an
    off-by-one that keeps two.
    """
    files = [_seed_bak(tmp_path, i) for i in range(1, 5)]
    sweep_bak_files(tmp_path)
    assert _existing_baks(tmp_path) == [files[-1].name]


def test_sweep_ignores_non_matching_filenames(tmp_path: Path) -> None:
    """Over-broad-glob guard: real `/data` siblings must survive untouched.

    Catches: a loosened pattern (a plain glob instead of the anchored regex,
    or ``.match()`` instead of ``.fullmatch()``) that would sweep up the live
    DB, a WAL sidecar, or an in-flight transfer temp.
    """
    older = _seed_bak(tmp_path, 1)
    newer = _seed_bak(tmp_path, 2)
    siblings = {
        "wxverify.db": b"live-db-placeholder",
        "wxverify.db-wal": b"wal-placeholder",
        "wxverify.db-shm": b"shm-placeholder",
        ".wxverify-import-deadbeefdeadbeefdeadbeefdeadbeef.db.tmp": b"import-temp",
        "wxverify-backup.db.bak": b"no-timestamp-shape",
        "wxverify-20000101-000009Z.db.bak.old": b"extra-suffix",
    }
    for name, content in siblings.items():
        (tmp_path / name).write_bytes(content)

    sweep_bak_files(tmp_path)

    for name, content in siblings.items():
        path = tmp_path / name
        assert path.exists(), f"{name} must survive an over-broad-glob guard"
        assert path.read_bytes() == content, f"{name} content must be untouched"
    assert not older.exists(), "the older MATCHING .bak must still be swept"
    assert newer.exists(), "the newer MATCHING .bak must survive"


def test_sweep_ignores_impossible_timestamp_names(tmp_path: Path) -> None:
    """A filename shaped like the regex but not a real calendar timestamp
    (``99999999-999999``) must never enter the candidate set at all.

    Before the ``datetime.strptime`` guard, ``_BAK_RE`` validated only digit
    *counts*, so this bogus name matched, and since candidates sort
    lexicographically descending on the raw timestamp string, the digit
    string ``"99999999-999999"`` sorts above every real timestamp and wins
    "newest" -- retaining the bogus file and deleting every genuine backup.
    All three files here are gate-passing valid databases, so that win would
    not be an artifact of the corrupt-newest early-return path.

    Catches: a regression that drops the ``datetime.strptime`` validation (or
    narrows its exception handling), letting a digit-shaped-but-impossible
    timestamp back into the sort.
    """
    older = _seed_bak(tmp_path, 1)
    newer = _seed_bak(tmp_path, 2)
    impossible = _make_valid_bak(tmp_path / "wxverify-99999999-999999Z.db.bak")

    sweep_bak_files(tmp_path)

    assert impossible.exists(), "impossible-timestamp name must never be a candidate"
    assert newer.exists(), "the newest GENUINE backup must survive"
    assert not older.exists(), "the older genuine backup must still be swept"


def test_sweep_orders_by_embedded_timestamp_not_mtime(tmp_path: Path) -> None:
    """Ordering must use the embedded filename timestamp, never `st_mtime`.

    Catches: an implementation that (wrongly) sorts by ``stat().st_mtime``
    instead of the filename timestamp.
    """
    import os
    import time

    older_name = _seed_bak(tmp_path, 1)  # older embedded timestamp
    newer_name = _seed_bak(tmp_path, 2)  # newer embedded timestamp
    # Give the OLDER-named file the NEWER mtime.
    now = time.time()
    os.utime(older_name, (now, now))
    os.utime(newer_name, (now - 3600, now - 3600))

    sweep_bak_files(tmp_path)

    assert not older_name.exists(), (
        "older-named-but-newer-mtime file must be deleted (mtime is not the "
        "ordering signal)"
    )
    assert newer_name.exists(), "newer-named file must survive regardless of mtime"


def test_sweep_never_deletes_the_just_created_file(tmp_path: Path) -> None:
    """`keep` is excluded unconditionally, even when it looks oldest by name.

    The fixture includes an additional matching `.bak` that sorts
    lexicographically NEWER than `keep` -- with `keep` alone on disk,
    `candidates` would be empty and both a correct and a guard-less
    implementation would take the same trivial early return, proving
    nothing. With the newer-named sibling present, a sort-order-only
    implementation picks THAT file as "newest" and deletes `keep`.

    Catches: a `keep` exclusion applied only as a side effect of sort order
    rather than as an unconditional guard.
    """
    keep = _seed_bak(tmp_path, 1)  # oldest-looking name
    newer_sibling = _seed_bak(tmp_path, 2)  # sorts newer than `keep`

    sweep_bak_files(tmp_path, keep=keep)

    assert keep.exists(), "keep must survive regardless of how it sorts by name"
    assert not newer_sibling.exists(), "every non-keep candidate must be swept"


def test_startup_sweep_spans_legacy_and_suffixed_bak_shapes(tmp_path: Path) -> None:
    """A `/data` inherited from <=0.8.9 holds bare-timestamp `.bak` files; a
    post-upgrade import writes suffixed ones. The startup sweep must treat
    both as candidates and prune to the single newest across the two shapes.

    Catches: a `_BAK_RE` that requires the uuid token, which drops every
    legacy file out of the candidate set silently (no log at any level) and
    pins it in /data forever -- the pre-0.9 accumulation bug, reintroduced
    for exactly the artifacts the retention sweep was shipped to clean up.
    """
    legacy = _make_valid_bak(tmp_path / "wxverify-20000101-000001Z.db.bak")
    suffixed = _make_valid_bak(tmp_path / "wxverify-20000101-000002-deadbeefZ.db.bak")

    sweep_bak_files(tmp_path)

    assert not legacy.exists(), "a pre-0.9 bare-timestamp .bak must still be swept"
    assert suffixed.exists(), "the newest across both shapes must survive"


def test_startup_sweep_legacy_shape_can_also_win_newest(tmp_path: Path) -> None:
    """Reverse ordering of `test_startup_sweep_spans_legacy_and_suffixed_bak_shapes`:
    a legacy bare-timestamp file with the LATER embedded timestamp must win
    "newest" over an older suffixed file -- proving the mixed-shape
    comparison is a real timestamp compare, not "suffixed always wins" (which
    the first test alone cannot distinguish, since there the suffixed file is
    also the newer one).
    """
    suffixed = _make_valid_bak(tmp_path / "wxverify-20000101-000001-deadbeefZ.db.bak")
    legacy = _make_valid_bak(tmp_path / "wxverify-20000101-000002Z.db.bak")

    sweep_bak_files(tmp_path)

    assert not suffixed.exists(), "the older suffixed file must still be swept"
    assert legacy.exists(), "a legacy bare-timestamp file can also win newest"


@pytest.mark.parametrize("corruption", ["zero_byte", "garbage_bytes"])
def test_sweep_skips_pruning_when_newest_is_corrupt(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, corruption: str
) -> None:
    """A corrupt "newest" `.bak` aborts the WHOLE sweep -- nothing is deleted.

    The zero-byte case is load-bearing: SQLite reports a 0-byte file as a
    valid, empty database (``quick_check``/``integrity_check`` both 'ok'), so
    only the ``user_version != 0`` / `_REQUIRED_TABLES` half of the gate
    rejects it. Garbage bytes is the shape ``quick_check`` alone already
    catches.

    Catches: a sweep that trusts filename/ordering alone, and a validity gate
    weaker than `_validate_upload`'s (``quick_check``-only would pass the
    zero-byte case and prune every good backup down to a corrupt one).
    """
    valid_1 = _seed_bak(tmp_path, 1)
    valid_2 = _seed_bak(tmp_path, 2)
    newest = tmp_path / _bak_name(_ts(3))
    if corruption == "zero_byte":
        newest.write_bytes(b"")
    else:
        newest.write_bytes(b"not a sqlite database, just garbage padding bytes")

    with caplog.at_level(logging.WARNING, logger="wxverify.api.routes.db_transfer"):
        sweep_bak_files(tmp_path)

    assert valid_1.exists(), "sweep must delete NOTHING when newest is corrupt"
    assert valid_2.exists(), "sweep must delete NOTHING when newest is corrupt"
    assert newest.exists()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("validity check" in r.getMessage() for r in warnings), (
        f"expected a validity-check warning; got {[r.getMessage() for r in warnings]}"
    )


def test_sweep_unlink_failure_does_not_raise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed unlink on one candidate logs and continues; the sweep call
    itself never raises, and OTHER deletable stale files are still removed.

    Catches: a sweep that aborts entirely (leaving later, deletable files
    un-swept) or one that silently drops the failure with no log line.
    """
    newest = _seed_bak(tmp_path, 3)
    failing = _seed_bak(tmp_path, 2)
    deletable = _seed_bak(tmp_path, 1)

    real_unlink = Path.unlink

    def _flaky_unlink(self: Path, *args: Any, **kwargs: Any) -> None:
        if self == failing:
            raise PermissionError("synthetic permission denied")
        real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", _flaky_unlink)

    with caplog.at_level(logging.WARNING, logger="wxverify.api.routes.db_transfer"):
        sweep_bak_files(tmp_path)  # must not raise

    assert newest.exists()
    assert failing.exists(), "the file whose unlink raised must remain"
    assert not deletable.exists(), "other stale files must still be swept"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(failing.name in r.getMessage() for r in warnings)


def test_post_creation_sweep_with_one_pre_existing_bak(tmp_path: Path) -> None:
    """The boundary the 3+-file tests pass over: exactly ONE pre-existing
    `.bak` alongside `keep`.

    Catches: a ``len(candidates) <= 1`` early return applied to the
    ``keep is not None`` branch, which silently skips this case entirely.
    """
    old = _seed_bak(tmp_path, 1)
    keep = _seed_bak(tmp_path, 2)

    sweep_bak_files(tmp_path, keep=keep)

    assert not old.exists()
    assert _existing_baks(tmp_path) == [keep.name]


# ---------------------------------------------------------------------------
# HTTP/lifespan integration tests (TestClient inline-BackgroundTask idiom).
# ---------------------------------------------------------------------------


def test_import_triggers_post_creation_sweep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """POSTing an import sweeps the pre-existing `.bak` pile down to the new
    backup only, via `_rebuild_derived`'s ``BackgroundTask`` (which
    `TestClient` runs inline before ``client.post`` returns).

    Catches: the sweep call never wired into `_rebuild_derived`, or a
    ``candidates[1:]``-style retention that leaves a stale file alive beside
    the new backup. Does NOT catch a real ``BackgroundTask`` being swapped for
    an inline pre-response call (or vice versa): `TestClient` (pinned
    ``starlette`` 0.46.2) drains background tasks before ``client.post()``
    returns, so both placements produce identical observable ordering here --
    post-response placement is a code-review property, not something a
    filesystem assertion in this test can prove.
    """
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Pre Import Site")
    conn.commit()
    db_dir = Path(config.db_path).parent
    for seq in (1, 2, 3):
        _seed_bak(db_dir, seq)
    assert len(_existing_baks(db_dir)) == 3

    b_path = _build_replacement_db(tmp_path, "source-b.db", "Post Import Site")
    payload = b_path.read_bytes()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
    assert resp.status_code == 200, f"{resp.status_code}: {resp.text}"
    new_backup = resp.json()["backup"]

    remaining = _existing_baks(db_dir)
    assert remaining == [new_backup], (
        f"expected only the new backup to survive; got {remaining}"
    )


def test_startup_sweep_cleans_pre_existing_pile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """5 pre-existing `.bak` files inherited from before this fix shipped are
    swept down to the newest at boot, with no import involved at all.

    Catches: a sweep that's only wired into the import path and never runs
    at boot -- the exact "existing pile never gets cleaned" gap in the brief.
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    files = [_seed_bak(db_dir, i) for i in range(1, 6)]
    assert len(_existing_baks(db_dir)) == 5

    app = _make_app(monkeypatch)
    with TestClient(app):
        pass  # lifespan startup (and shutdown) is all this test needs.

    assert _existing_baks(db_dir) == [files[-1].name]


def test_startup_sweep_failure_does_not_abort_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A raising startup sweep must not prevent the add-on from starting.

    Catches: a missing or too-narrow ``try/except`` around the startup sweep
    call in `lifespan()`.
    """
    _init_tmp_db(tmp_path)

    def _raise(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("synthetic startup sweep failure")

    monkeypatch.setattr(db_transfer, "sweep_bak_files", _raise)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/api/csrf")
        assert resp.status_code == 200, "app must still be usable after startup"


# ---------------------------------------------------------------------------
# backup_exclude static content.
# ---------------------------------------------------------------------------


def test_config_yaml_excludes_bak_files() -> None:
    """`config.yaml` must declare `backup_exclude: ["*.db.bak"]` verbatim.

    Catches: the key being added with a wrong/looser glob (e.g. ``"*.bak"``)
    or silently dropped/typo'd (``backup_excludes``, wrong key name).
    """
    repo = Path(__file__).resolve().parents[1]
    config_text = (repo / "config.yaml").read_text(encoding="utf-8")
    match = re.search(r"^backup_exclude:\n((?:  - .*\n)+)", config_text, re.MULTILINE)
    assert match is not None, "backup_exclude key with a YAML list must be present"
    entries = [line.strip()[2:] for line in match.group(1).splitlines()]
    assert '"*.db.bak"' in entries, (
        f'expected "*.db.bak" in backup_exclude; got {entries}'
    )


# Sanity: `_looks_like_valid_sqlite` is exercised indirectly by every sweep
# test above via the newest-file validity gate; this direct check pins its
# own contract independent of the sweep's control flow.
def test_looks_like_valid_sqlite_rejects_zero_byte_file(tmp_path: Path) -> None:
    empty = tmp_path / "empty.db.bak"
    empty.write_bytes(b"")
    assert _looks_like_valid_sqlite(empty) is False
    valid = _make_valid_bak(tmp_path / "valid.db.bak")
    assert _looks_like_valid_sqlite(valid) is True
