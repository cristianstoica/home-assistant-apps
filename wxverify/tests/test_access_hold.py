"""Unit tests for ``wxverify.worker.access_hold`` (plan 2026-10-27
refused-access-hold §13.2, AH1-AH5).

Five oracles, each pinning one contract of the hold primitives directly
(no job, no HTTP call):

AH1 — the hold boundary is strict ``<``, not ``<=``, and lasts exactly
      ``ACCESS_HOLD_SECONDS`` (3600 s).
AH2 — a repeat refusal preserves ``since`` from the first refusal of the
      run, but always refreshes ``until``/``status``/``message``.
AH3 — ``check_access_hold`` is read-only: the stored row is byte-identical
      before and after, whether or not it raises.
AH4 — an unreadable ``until`` fails OPEN (``access_held`` returns None) and
      logs a WARNING, rather than holding forever.
AH5 — each kind has its own key: a hold on one kind never holds another.

Isolation: a bare in-memory ``sqlite3.Connection`` (``create_schema`` only —
these primitives touch only ``runtime_state``, so no site/station/sources
seed is needed). Time control: ``wxverify.worker.access_hold.utc_now`` is
monkeypatched directly (bare import in that module; see
tests/test_fetch_obs_partial_cycle.py's ``_freeze`` docstring for why this
module needs its own patch rather than relying on a patch to
``wxverify.core.timeutil.utc_now``).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from wxverify.db.migrations import create_schema
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.worker.access_hold import (
    ACCESS_HOLD_SECONDS,
    AccessHeld,
    access_held,
    access_hold_key,
    check_access_hold,
    clear_access_refused,
    read_access_record,
    record_access_refused,
)

# ---------------------------------------------------------------------------
# DB fixture helper
# ---------------------------------------------------------------------------


def _make_conn() -> sqlite3.Connection:
    """Open an in-memory schema-current DB; these primitives need only
    ``runtime_state``, so no site/station/sources seed is required."""
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    return conn


def _runtime_state_row(
    conn: sqlite3.Connection, key: str
) -> tuple[str, str, str] | None:
    """The full ``(key, value, updated_at)`` row, or None — for a
    byte-identical before/after comparison that also catches a touched
    ``updated_at`` with an unchanged ``value``."""
    row = conn.execute(
        "SELECT key, value, updated_at FROM runtime_state WHERE key = ?", (key,)
    ).fetchone()
    return None if row is None else (str(row[0]), str(row[1]), str(row[2]))


_NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)


def _freeze(monkeypatch: pytest.MonkeyPatch, now: datetime = _NOW) -> None:
    """Patch access_hold's own bare ``utc_now()`` import to a fixed value."""
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: now)


# ---------------------------------------------------------------------------
# AH1 — hold boundary is strict `<`, not `<=`; lasts exactly 3600 s
# ---------------------------------------------------------------------------


def test_ah1_hold_boundary_is_strict_less_than_and_3600_seconds_long(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The hold's ``until`` is exclusive (held while ``now < until``, not
    ``<=``), and its length is exactly ``ACCESS_HOLD_SECONDS``.

    Every stored field is pinned to its literal value, not just its presence.

    Mutant (boundary): ``access_held``'s ``if now < parse_utc(record.until)``
    changed to ``<=`` -- at the exactly-at-``until`` assertion below:
    correct = None, mutant = the stored ``until`` string (non-None).

    Mutant (length): ``ACCESS_HOLD_SECONDS`` changed from 3600 to 1800 -- at
    the "until literal" assertion below: correct =
    ``"2026-07-10T13:00:00Z"``, mutant = ``"2026-07-10T12:30:00Z"``.

    Mutant (status/message swap): ``record_access_refused`` passed the
    wrong ``status`` into ``message`` (or vice versa) -- at the "status"
    and "message" assertions below: correct = ``401`` /
    ``"access refused (HTTP 401)"``, mutant = a mismatched pair.
    """
    conn = _make_conn()
    _freeze(monkeypatch, _NOW)
    until = record_access_refused(conn, "hourly", 401)

    assert ACCESS_HOLD_SECONDS == 3600
    record = read_access_record(conn, "hourly")
    assert record is not None
    assert record.since == "2026-07-10T12:00:00Z"
    assert record.until == "2026-07-10T13:00:00Z"
    assert record.status == 401
    assert record.message == "access refused (HTTP 401)"
    assert until == record.until

    # Paired positive: clearly inside the window -> held.
    assert access_held(conn, "hourly", _NOW + timedelta(seconds=1800)) == until

    # One second before the boundary -> still held.
    assert access_held(conn, "hourly", _NOW + timedelta(seconds=3599)) == until

    # Exactly at the boundary -> NOT held (strict `<`).
    assert access_held(conn, "hourly", _NOW + timedelta(seconds=3600)) is None, (
        "at exactly `until`, the hold must have ended (strict `<`)"
    )

    # One second after the boundary -> NOT held.
    assert access_held(conn, "hourly", _NOW + timedelta(seconds=3601)) is None


# ---------------------------------------------------------------------------
# AH2 — `since` preserved across refusals; `until`/status/message refreshed
# ---------------------------------------------------------------------------


def test_ah2_since_preserved_across_refusals_but_status_and_until_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second refusal, at T + 2h, keeps the first refusal's ``since`` (T)
    but refreshes ``until`` to exactly T + 3h, and ``status``/``message`` to
    the second refusal's own values.

    Paired positive: the FIRST refusal's ``since`` is asserted to equal that
    refusal's own ``now`` (so "preserved" below is not vacuous -- `since`
    really is capable of taking a fresh value).

    Mutant: ``record_access_refused``'s ternary
    ``since = existing.since if (...) else isoformat_utc(now)`` replaced by
    always ``since = isoformat_utc(now)`` -- at the "since preserved"
    assertion below: correct = ``"2026-07-10T12:00:00Z"`` (T0), mutant =
    ``"2026-07-10T14:00:00Z"`` (T1, the second refusal's own now).
    """
    conn = _make_conn()
    t0 = _NOW
    _freeze(monkeypatch, t0)
    record_access_refused(conn, "current_obs", 401)
    first = read_access_record(conn, "current_obs")
    assert first is not None and first.since is not None
    assert first.since == "2026-07-10T12:00:00Z", (
        "paired positive: the first refusal must set since to its own now"
    )

    t1 = t0 + timedelta(hours=2)
    _freeze(monkeypatch, t1)
    record_access_refused(conn, "current_obs", 403)
    second = read_access_record(conn, "current_obs")
    assert second is not None

    assert second.since == "2026-07-10T12:00:00Z", (
        f"since must be preserved across refusals: first={first.since!r},"
        f" second={second.since!r}"
    )
    assert second.until == "2026-07-10T15:00:00Z", (
        f"until must refresh to T + 3h, got {second.until!r}"
    )
    assert second.status == 403, f"status must refresh to 403, got {second.status!r}"
    assert second.message == "access refused (HTTP 403)"


# ---------------------------------------------------------------------------
# AH3 — check_access_hold is read-only (byte-identical row before/after)
# ---------------------------------------------------------------------------


def test_ah3_check_access_hold_never_writes_the_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``check_access_hold`` never touches the stored row, whether it
    raises (held) or returns (not held).

    Paired positive: ``record_access_refused`` (called both before and
    after the two read-only checks) DOES change the row -- proving the
    byte-identical comparisons below are not vacuously true because nothing
    could ever change this row.

    Mutant: ``check_access_hold`` touches the row before raising (e.g. a
    spurious re-``set_runtime_state`` of its own current value) -- at the
    "row unchanged while held" assertion below: correct = the row's
    original ``updated_at``, mutant = a later ``updated_at`` (same ``kind``
    key, SQLite's own clock stamps it on any write even when the JSON value
    is unchanged). Also catches a held check that rewrites the row,
    extending or touching the hold: the held check below runs at
    T + 30 min, not at the refusal's own instant, so a rewrite (e.g. calling
    ``record_access_refused`` again before raising) would stamp a new
    ``updated_at`` and/or move ``until`` -- caught by the same
    "row unchanged while held" assertion.
    """
    conn = _make_conn()
    t0 = _NOW
    _freeze(monkeypatch, t0)
    until = record_access_refused(conn, "current_obs", 401)
    key = access_hold_key("current_obs")
    before_held_check = _runtime_state_row(conn, key)
    assert before_held_check is not None

    # Held: must raise AND must not touch the row. Checked 30 min after the
    # refusal, not at the same instant, so a held check that rewrites the
    # row (extending or touching the hold) cannot hide behind a same-time
    # coincidence.
    _freeze(monkeypatch, t0 + timedelta(minutes=30))
    with pytest.raises(AccessHeld) as info:
        check_access_hold(conn, "current_obs")
    assert info.value.until == until
    assert _runtime_state_row(conn, key) == before_held_check, (
        "check_access_hold must not write the row while the hold is in force"
    )

    # Not held (now past until): must return (no raise) AND must not touch
    # the row either.
    past = datetime.fromisoformat(until.replace("Z", "+00:00")) + timedelta(seconds=1)
    _freeze(monkeypatch, past)
    check_access_hold(conn, "current_obs")  # must not raise
    assert _runtime_state_row(conn, key) == before_held_check, (
        "check_access_hold must not write the row once the hold has ended"
    )

    # Paired positive: a real refusal DOES change the row, proving the two
    # byte-identical checks above were comparing a row that is actually
    # capable of changing.
    record_access_refused(conn, "current_obs", 403)
    assert _runtime_state_row(conn, key) != before_held_check


# ---------------------------------------------------------------------------
# AH4 — an unreadable `until` fails OPEN and logs a WARNING
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case_name,write_row",
    [
        (
            "non-json-value",
            lambda conn, key: set_runtime_state(conn, key, "not json"),
        ),
        (
            "unreadable-until",
            lambda conn, key: set_runtime_state(
                conn,
                key,
                json.dumps(
                    {
                        "since": "x",
                        "until": "not-a-stamp",
                        "status": 401,
                        "message": "m",
                    }
                ),
            ),
        ),
    ],
)
def test_ah4_unreadable_until_fails_open_and_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    case_name: str,
    write_row: Callable[[sqlite3.Connection, str], None],
) -> None:
    """A row that is not a JSON object (``"not json"``), and a row whose
    ``until`` does not parse (``"not-a-stamp"``), must each fail OPEN
    (``access_held`` returns None, so the kind is NOT held) -- logged at
    exactly one WARNING per ``access_held`` call.

    Paired positive: the same hold kind, written readably via
    ``record_access_refused``, with ``now`` inside the window, IS held --
    proving the None result above is meaningful rather than a vacuous
    always-None stub.

    Mutant: ``access_held``'s ``if record is None or record.until is None:
    return None`` changed to fail CLOSED for the unreadable case (return a
    synthesized far-future ``until`` instead of None) -- at the "fails
    open" assertion below: correct = None, mutant = a non-None iso string
    (e.g. now + 24h).
    """
    conn = _make_conn()
    now = _NOW
    _freeze(monkeypatch, now)

    key = access_hold_key("current_obs")
    write_row(conn, key)

    caplog.clear()
    caplog.set_level(logging.WARNING, logger="wxverify.worker.access_hold")
    result = access_held(conn, "current_obs", now)
    assert result is None, (
        f"[{case_name}] an unreadable row must fail OPEN (not held), not closed"
    )
    warnings = [
        r
        for r in caplog.records
        if r.name == "wxverify.worker.access_hold" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1, (
        f"[{case_name}] exactly one WARNING must be logged by this access_held"
        f" call, got {len(warnings)}"
    )
    assert (
        "unreadable weather.com access hold kind=current_obs"
        in warnings[0].getMessage()
    ), f"[{case_name}] the WARNING must name the kind"

    # Paired positive: a readable hold on the SAME kind, now inside the
    # window, IS held.
    caplog.clear()
    record_access_refused(conn, "current_obs", 401)
    assert access_held(conn, "current_obs", now + timedelta(seconds=10)) is not None, (
        f"[{case_name}] paired positive: a readable hold inside the window IS held"
    )


# ---------------------------------------------------------------------------
# AH5 — per-kind isolation: a shared key would be wrong
# ---------------------------------------------------------------------------


def test_ah5_hold_on_one_kind_never_holds_another(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refusal of ``hourly`` must not hold ``current_obs`` or
    ``five_minute``: each kind's hold lives under its own ``runtime_state``
    key. Clearing a DIFFERENT kind (``current_obs``, which has no row) must
    leave ``hourly``'s row byte-identical and still held.

    Paired positive: ``hourly`` itself IS held by its own refusal --
    proving the None results for the other two kinds are not vacuous (the
    refusal demonstrably can hold a kind; it just must not hold these).

    Mutant: ``access_hold_key(kind)`` ignores ``kind`` and returns one
    shared key for every kind -- at the "current_obs not held" assertion
    below: correct = None, mutant = the stored ``until`` string
    (current_obs would read the same shared row hourly just wrote).

    Mutant (clear bleeds across kinds): ``clear_access_refused`` deletes
    every kind's row instead of only its argument's -- at the "hourly row
    unchanged after clearing current_obs" assertion below: correct = the
    row's original ``(key, value, updated_at)``, mutant = None (the row
    gone).
    """
    conn = _make_conn()
    now = _NOW
    _freeze(monkeypatch, now)
    record_access_refused(conn, "hourly", 401)

    check_at = now + timedelta(seconds=10)
    assert access_held(conn, "hourly", check_at) is not None, (
        "paired positive: hourly itself must be held by its own refusal"
    )
    assert access_held(conn, "current_obs", check_at) is None, (
        "an hourly refusal must not hold current_obs -- per-kind isolation"
    )
    assert access_held(conn, "five_minute", check_at) is None, (
        "an hourly refusal must not hold five_minute -- per-kind isolation"
    )
    # Distinct storage keys, not just distinct runtime values.
    assert access_hold_key("hourly") != access_hold_key("current_obs")
    assert access_hold_key("hourly") != access_hold_key("five_minute")
    assert get_runtime_state(conn, access_hold_key("current_obs")) is None

    hourly_key = access_hold_key("hourly")
    before = _runtime_state_row(conn, hourly_key)
    assert before is not None

    clear_access_refused(conn, "current_obs")

    assert _runtime_state_row(conn, hourly_key) == before, (
        "clearing a different kind must leave hourly's row byte-identical"
    )
    assert access_held(conn, "hourly", check_at) is not None, (
        "hourly must still be held after a different kind was cleared"
    )
