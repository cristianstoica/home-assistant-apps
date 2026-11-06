"""The lane gate (daily-close switch-over plan §6.2; test IDs GA1-GA6 of
§17.4).

GA1, GA4, GA5 and GA6 drove ``wxverify.worker.cutover`` and the lane's
cutover-attempt start inside ``_run_main_lane`` -- both removed now that the
daily close is permanent (storage-release plan 2026-11-02, table 1.B) and
there is no longer a switch-over to run. Those four are deleted with the
module.

GA2 and GA3 cover the surviving, permanent gate itself
(``wxverify.db.site_days.daily_close_on`` and
``wxverify.worker.processor._claim_if_on``), unrelated to the removed
cutover machinery, and stay as written.

Synthetic data only: no real station id, coordinate, site or zone name.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final

import pytest

from tests.helpers import asof_make_site, close_off
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.queue import enqueue_if_absent
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY
from wxverify.worker import scheduler as scheduler_module

# T = C 10:00Z (plan §4.1), as in tests/test_cutover.py.
_C: Final = date(2025, 9, 1)
_T: Final = datetime(_C.year, _C.month, _C.day, 10, 0, 0, tzinfo=UTC)


def _init_tmp_db(tmp_path: Path) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    return init_db(str(db_path))


def _make_site(conn: sqlite3.Connection, *, timezone: str, enabled: bool = True) -> int:
    site_id = asof_make_site(conn, f"lane-site-{timezone}-{enabled}")
    conn.execute(
        "UPDATE sites SET timezone = ?, enabled = ? WHERE id = ?",
        (timezone, 1 if enabled else 0, site_id),
    )
    return site_id


def test_ga2_scheduler_tick_returns_immediately_while_the_key_is_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    close_off(conn)
    _make_site(conn, timezone="UTC")
    # After 07:00 local (methodology.SNAPSHOT_LOCAL_TIME, the default
    # snapshot time): _enqueue_due_forecast_records' gap-scan branch has no
    # daily_close_on guard of its own and needs no feed or station, so it
    # enqueues the moment scheduler_tick's own early return is bypassed --
    # the condition this test exists to rule out.
    now = datetime(2025, 1, 2, 8, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(scheduler_module, "utc_now", lambda: now)

    assert get_runtime_state(conn, DAILY_CLOSE_ON_KEY) is None

    scheduler_module.scheduler_tick(conn)

    verification_rows = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'verification_run'"
    ).fetchone()[0]
    fetch_feed_rows = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'fetch_feed'"
    ).fetchone()[0]
    assert verification_rows == 0, (
        f"M18 -> at this assertion: correct = 0 verification_run rows, "
        f"mutant = {verification_rows}"
    )
    assert fetch_feed_rows == 0, (
        f"M18 -> at this assertion: correct = 0 fetch_feed rows (the early "
        f"return skipped every enqueue while the key is absent), "
        f"mutant = {fetch_feed_rows}"
    )
    total_rows = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    assert total_rows == 0, (
        f"M18 -> at this assertion: correct = 0 jobs rows of any type (the "
        f"early return skips every enqueue in the function while the key is "
        f"absent, not just the two checked above), mutant = {total_rows}"
    )

    # Paired positive, at the SAME clock reading: the same "COUNT(*) FROM
    # jobs" oracle, run once the key is actually set, must see real enqueued
    # work -- proving the "0 rows" read above can tell "nothing ran" from
    # "something ran", not report zero unconditionally.
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(now))
    conn.commit()
    scheduler_module.scheduler_tick(conn)
    total_rows_once_on = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    assert total_rows_once_on >= 1, (
        "M18 -> paired positive: correct = at least one jobs row once the "
        f"key is set, got {total_rows_once_on} (the same read used above "
        "can see real enqueued work)"
    )


def test_ga3_claim_if_on_returns_none_while_absent_then_claims_once_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.worker.processor import _claim_if_on  # noqa: PLC2701

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    close_off(conn)
    site_id = _make_site(conn, timezone="UTC")
    enqueue_if_absent(conn, "fetch_feed", site_id, "ga3-feed", {})
    conn.commit()

    result = _claim_if_on(conn)
    assert result is None, (
        f"M19 -> at this assertion: correct = None while the key is "
        f"absent, mutant = {result!r}"
    )
    status = conn.execute(
        "SELECT status FROM jobs WHERE job_key = 'ga3-feed'"
    ).fetchone()[0]
    assert status == "pending", (
        f"M19 -> at this assertion: correct = 'pending' (nothing claimed "
        f"while absent), mutant = {status!r}"
    )

    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(_T))
    conn.commit()

    claimed = _claim_if_on(conn)
    assert claimed is not None
    assert claimed.job_key == "ga3-feed"
