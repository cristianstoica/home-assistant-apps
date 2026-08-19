"""Item 5 (§1.2/D16): the import limit is disclosed in two places, and the
four refusal sites do not all measure the same thing.

Anchors: ``wxverify/api/routes/db_transfer.py`` (``MAX_IMPORT_BYTES`` /
``MAX_IMPORT_MIB`` at :60-61, the wire-length 413 at :479, and the three
post-inflate 413s at :579/:593/:607); the function-local import in
``wxverify/web/routes.py`` (so ``monkeypatch.setattr(db_transfer,
"MAX_IMPORT_MIB", ...)`` is visible per request); and
``wxverify/web/templates/ops/_import.html`` (``id="import-limit-note"``).

Harness idiom copied verbatim from ``tests/test_db_transfer.py``: per-test
tmp DB via ``_init_tmp_db``, idle-worker app, real ``TestClient``.

Synthetic data only (public repo): no real site/station identifiers.
"""

from __future__ import annotations

import asyncio
import gzip
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.routes import db_transfer
from wxverify.db.connection import close_db, init_db

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_db_transfer.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
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


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {
        "Origin": "http://testserver",
        "X-CSRF-Token": token,
        "Content-Type": "application/octet-stream",
    }


def _import_limit_note(html: str) -> str:
    marker = 'id="import-limit-note"'
    assert marker in html, "the Ops page renders no import-limit-note element"
    start = html.index(marker)
    end = html.index("</p>", start)
    return html[start:end]


# ---------------------------------------------------------------------------
# O31 — the panel discloses the limit, derived from the constant, rendered.
# ---------------------------------------------------------------------------


def test_panel_discloses_the_patched_limit_in_rendered_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M24 target: a hard-coded '256' in the template renders '256' against
    the patched '7' and fails here. Patches MAX_IMPORT_MIB, not
    MAX_IMPORT_BYTES -- the panel renders the former (§6.11); patching the
    latter would produce a false FAIL against correct code."""
    _init_tmp_db(tmp_path)
    monkeypatch.setattr(db_transfer, "MAX_IMPORT_MIB", 7)
    app = _make_app(monkeypatch)

    with TestClient(app) as client:
        html = client.get("/ops").text

    note = _import_limit_note(html)
    assert "7 MiB" in note
    assert "after decompression" in note
    assert "refused once expanded" in note


def test_mib_constant_is_derived_from_the_byte_cap() -> None:
    """Unpatched derivation pin: keeps the property O31's kill column
    claims -- that MAX_IMPORT_MIB is not an independent literal."""
    assert db_transfer.MAX_IMPORT_MIB == db_transfer.MAX_IMPORT_BYTES // (1024 * 1024)


# ---------------------------------------------------------------------------
# O32 — the wire refusal, over rendered HTTP response JSON.
# ---------------------------------------------------------------------------


def test_wire_refusal_names_the_limit_and_the_upload_not_decompression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M25 target: a single shared message would tell the operator the wire
    check measured a decompressed database."""
    _init_tmp_db(tmp_path)
    monkeypatch.setattr(db_transfer, "MAX_IMPORT_BYTES", 64)
    monkeypatch.setattr(db_transfer, "MAX_IMPORT_MIB", 1)
    app = _make_app(monkeypatch)

    with TestClient(app, raise_server_exceptions=False) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=b"x" * 200, headers=headers)

    assert resp.status_code == 413
    body = resp.json()
    error = body["error"]
    assert str(db_transfer.MAX_IMPORT_MIB) in error
    assert "upload" in error
    assert "decompression" not in error


# ---------------------------------------------------------------------------
# O33 — the after-inflate refusal, over rendered HTTP response JSON.
# ---------------------------------------------------------------------------


def test_after_inflate_refusal_names_the_limit_and_what_it_measured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A highly-compressible gzip whose compressed size is far under a
    lowered cap and whose inflated size crosses it: admits the wire check
    (:479) and refuses mid-inflate (:579-ish), the path D16 requires. An
    incompressible fixture is wrong here -- its compressed size also
    exceeds the cap, tripping the wire check first (O32's message)."""
    _init_tmp_db(tmp_path)
    monkeypatch.setattr(db_transfer, "MAX_IMPORT_BYTES", 4096)
    monkeypatch.setattr(db_transfer, "MAX_IMPORT_MIB", 7)
    app = _make_app(monkeypatch)

    # Same construction as tests/test_db_transfer.py:3192's zip-bomb fixture.
    bomb = gzip.compress(b"\x00" * (2 * 1024 * 1024))
    assert len(bomb) < 4096, "the compressed body must fit under the wire cap"

    with TestClient(app, raise_server_exceptions=False) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=bomb, headers=headers)

    assert resp.status_code == 413
    error = resp.json()["error"]
    assert "7" in error
    assert "measured after any decompression" in error


# ---------------------------------------------------------------------------
# O34 — the uncompressed refusal, and the two message families diverge.
# ---------------------------------------------------------------------------


def test_uncompressed_refusal_carries_the_write_side_string_and_diverges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plain (non-gzip) body over the lowered cap with no Content-Length
    set (a generator body), so the byte counter -- not the header -- decides.
    Asserted unequal to the wire-side string O32 saw, so the two message
    families cannot silently collapse into one."""
    _init_tmp_db(tmp_path)
    monkeypatch.setattr(db_transfer, "MAX_IMPORT_BYTES", 64)
    monkeypatch.setattr(db_transfer, "MAX_IMPORT_MIB", 1)
    app = _make_app(monkeypatch)

    def _oversized_body() -> Iterator[bytes]:
        for _ in range(5):
            yield b"x" * 1000

    with TestClient(app, raise_server_exceptions=False) as client:
        wire_resp = client.post(
            "/api/import/db", content=b"x" * 200, headers=_csrf_headers(client)
        )
        body_resp = client.post(
            "/api/import/db", content=_oversized_body(), headers=_csrf_headers(client)
        )

    assert wire_resp.status_code == 413
    assert body_resp.status_code == 413
    wire_error = wire_resp.json()["error"]
    body_error = body_resp.json()["error"]

    assert "measured after any decompression" in body_error
    assert body_error != wire_error
    assert body_resp.json() == {"error": body_error}
