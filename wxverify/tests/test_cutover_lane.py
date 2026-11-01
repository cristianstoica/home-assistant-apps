"""The lane gate (daily-close switch-over plan §6.2; test IDs GA1-GA6 of
§17.4).

Written before P6's gate exists in ``worker/processor.py`` and
``worker/scheduler.py``: every test here reaches a name P6 adds --
``wxverify.worker.cutover`` itself, or ``daily_close_on``/``Cutover``/
``_claim_if_on`` bound inside ``wxverify.worker.processor`` -- inside its
own body, so each fails today at that point (an import error, or
``AttributeError`` from ``monkeypatch.setattr`` on a name that does not yet
exist there), never at collection and never on a fixture or typo. GA2 is the
exception: ``scheduler_tick`` already exists, so it fails on a plain
assertion against today's unchanged (enqueuing) behaviour instead.

Lane rows run ``_run_main_lane`` with ``CUTOVER_RETRY_SECONDS`` and
``POLL_INTERVAL`` set to 0 and end it with a wrapper that raises a
test-local ``_Stop`` on its N-th call (plan §17.1), turning the otherwise
infinite loop into one deterministic pass.

Synthetic data only: no real station id, coordinate, site or zone name.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import pytest

from tests.helpers import asof_make_site
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.queue import enqueue_if_absent
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY
from wxverify.worker import processor as processor_module
from wxverify.worker import scheduler as scheduler_module

# T = C 10:00Z (plan §4.1), as in tests/test_cutover.py.
_C: Final = date(2025, 9, 1)
_T: Final = datetime(_C.year, _C.month, _C.day, 10, 0, 0, tzinfo=UTC)


class _Stop(Exception):
    """Ends ``_run_main_lane``'s infinite loop at a chosen, deterministic
    point (plan §17.1); carries whatever the stopping call wants to hand
    back to the test.
    """

    def __init__(self, payload: Any = None) -> None:
        super().__init__()
        self.payload = payload


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


class _PublicationStandIn:
    """Records every call to P4's two seams (plan §17.1) and writes one
    marker row per site when it "builds" a publication, instead of running
    P4's real build. Can be told to raise, once, for a site.
    """

    def __init__(self) -> None:
        self.build_calls: list[tuple[int, date]] = []
        self.ready_calls: list[tuple[int, date]] = []
        self._built: dict[int, date] = {}
        self._raise_once: dict[int, BaseException] = {}

    def raise_once(self, site_id: int, exc: BaseException) -> None:
        self._raise_once[site_id] = exc

    async def build(
        self, db: Any, writer: Any, site_id: int, first_record_date: date
    ) -> None:
        self.build_calls.append((site_id, first_record_date))
        if site_id in self._raise_once:
            exc = self._raise_once.pop(site_id)
            raise exc
        if self._built.get(site_id) != first_record_date:

            def _mark(
                conn: sqlite3.Connection,
                sid: int = site_id,
                frd: date = first_record_date,
            ) -> None:
                conn.execute(
                    "INSERT INTO cutover_test_markers (site_id, first_record_date)"
                    " VALUES (?, ?)",
                    (sid, frd.isoformat()),
                )

            await writer.write(_mark)
            self._built[site_id] = first_record_date

    def ready(
        self, conn: sqlite3.Connection, site_id: int, first_record_date: date
    ) -> bool:
        self.ready_calls.append((site_id, first_record_date))
        return self._built.get(site_id) == first_record_date


def _lane_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Database, sqlite3.Connection, _PublicationStandIn, tuple[int, int, int]]:
    """The CV fixture (plan §17.3) plus the lane's own zero-delay knobs."""
    from wxverify.worker import cutover as cutover_module

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    conn.execute(
        "CREATE TABLE cutover_test_markers (site_id INTEGER, first_record_date TEXT)"
    )

    site_1 = _make_site(conn, timezone="UTC")
    site_2 = _make_site(conn, timezone="Etc/GMT+5")
    site_3 = _make_site(conn, timezone="UTC", enabled=False)

    monkeypatch.setattr(cutover_module, "utc_now", lambda: _T)
    monkeypatch.setattr(cutover_module, "CUTOVER_RETRY_SECONDS", 0)
    monkeypatch.setattr(processor_module, "POLL_INTERVAL", 0)

    stand_in = _PublicationStandIn()
    monkeypatch.setattr(cutover_module, "build_first_publication", stand_in.build)
    monkeypatch.setattr(cutover_module, "first_publication_ready", stand_in.ready)

    return db, conn, stand_in, (site_1, site_2, site_3)


def test_ga1_the_gate_runs_before_the_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db, conn, stand_in, (s1, s2, s3) = _lane_fixture(tmp_path, monkeypatch)

    enqueue_if_absent(conn, "fetch_feed", s1, "ga1-feed", {})
    enqueue_if_absent(conn, "fetch_obs", s1, "wind-days", {})
    conn.commit()

    async def _stop_on_claimed_job(db_arg: Any, job: Any, *, lane: str) -> None:
        raise _Stop(job)

    monkeypatch.setattr(processor_module, "run_claimed_job", _stop_on_claimed_job)

    with pytest.raises(_Stop) as exc_info:
        asyncio.run(processor_module._run_main_lane(db))

    claimed_job = exc_info.value.payload
    assert get_runtime_state(conn, DAILY_CLOSE_ON_KEY) is not None, (
        "M17 -> at this assertion: correct = the key already set at the "
        "first run_claimed_job call, mutant = still absent (the gate ran "
        "after the claim)"
    )
    other_statuses = {
        row[0]
        for row in conn.execute(
            "SELECT status FROM jobs WHERE id != ?", (claimed_job.id,)
        ).fetchall()
    }
    assert other_statuses <= {"pending"}, (
        f"M17 -> at this assertion: correct = every other job still "
        f"pending, mutant = {other_statuses} (a job ran before the first "
        f"run_claimed_job call)"
    )


def test_ga2_scheduler_tick_returns_immediately_while_the_key_is_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    _make_site(conn, timezone="UTC")
    now = datetime(2025, 1, 2, 5, 0, 0, tzinfo=UTC)
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


def test_ga3_claim_if_on_returns_none_while_absent_then_claims_once_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.worker.processor import _claim_if_on  # noqa: PLC2701

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
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


def test_ga4_the_claim_rereads_the_key_instead_of_trusting_the_loops_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _make_site(conn, timezone="UTC")
    enqueue_if_absent(conn, "fetch_feed", site_id, "ga4-feed", {})
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(_T))
    conn.commit()

    monkeypatch.setattr(processor_module, "POLL_INTERVAL", 0)
    monkeypatch.setattr(processor_module, "scheduler_tick", lambda c: None)

    real_daily_close_on = processor_module.daily_close_on
    call_count = {"n": 0}

    def _wrapped(conn_arg: sqlite3.Connection) -> bool:
        call_count["n"] += 1
        result = real_daily_close_on(conn_arg)
        if call_count["n"] == 1:
            # A second connection deletes the key right after the loop's
            # own gate read returns -- a test-controlled injection landing
            # exactly between that read and the claim's own re-read.
            second_conn = sqlite3.connect(str(config.db_path), isolation_level=None)
            try:
                second_conn.execute(
                    "DELETE FROM runtime_state WHERE key = ?", (DAILY_CLOSE_ON_KEY,)
                )
            finally:
                second_conn.close()
        return result

    monkeypatch.setattr(processor_module, "daily_close_on", _wrapped)

    class _FakeCutover:
        calls = 0

        def __init__(self, db_arg: Any) -> None:
            pass

        async def advance(self) -> Any:
            _FakeCutover.calls += 1
            raise _Stop(_FakeCutover.calls)

    monkeypatch.setattr(processor_module, "Cutover", _FakeCutover)

    with pytest.raises(_Stop):
        asyncio.run(processor_module._run_main_lane(db))

    assert _FakeCutover.calls == 1, (
        f"M20 -> at this assertion: correct = the cutover runs on the "
        f"NEXT loop, once the key is genuinely absent, mutant = "
        f"{_FakeCutover.calls} cutover attempts by then"
    )
    status = conn.execute(
        "SELECT status FROM jobs WHERE job_key = 'ga4-feed'"
    ).fetchone()[0]
    assert status == "pending", (
        f"M20 -> at this assertion: correct = 'pending' (the claim's own "
        f"re-read saw the key absent), mutant = {status!r} (the claim "
        f"trusted the loop's stale read and ran the job anyway)"
    )


def test_ga5_the_heartbeat_runs_before_every_cutover_unit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.worker import cutover as cutover_module

    db, conn, stand_in, (s1, s2, s3) = _lane_fixture(tmp_path, monkeypatch)

    calls: list[str] = []
    real_heartbeat = processor_module._maybe_stamp_runtime_heartbeat  # noqa: SLF001

    async def _wrapped_heartbeat(
        db_arg: Database, key: str, last_stamp_at: float, now: float
    ) -> float:
        calls.append(f"heartbeat:{key}")
        return await real_heartbeat(db_arg, key, last_stamp_at, now)

    monkeypatch.setattr(
        processor_module, "_maybe_stamp_runtime_heartbeat", _wrapped_heartbeat
    )

    real_advance = cutover_module.Cutover.advance

    async def _wrapped_advance(self: Any) -> Any:
        calls.append("advance")
        step = await real_advance(self)
        if step is cutover_module.CutoverStep.ON:
            raise _Stop(step)
        return step

    monkeypatch.setattr(cutover_module.Cutover, "advance", _wrapped_advance)

    with pytest.raises(_Stop):
        asyncio.run(processor_module._run_main_lane(db))

    advance_indices = [i for i, c in enumerate(calls) if c == "advance"]
    assert advance_indices, "the cutover never ran"
    for idx in advance_indices:
        assert idx > 0 and calls[idx - 1].startswith("heartbeat:"), (
            f"M21 -> at this assertion (advance call at {idx}): correct = "
            f"a worker-loop heartbeat call immediately before it, "
            f"mutant = {calls[max(idx - 1, 0)]!r}"
        )


def test_ga6_a_failed_attempt_waits_then_a_fresh_attempt_reaches_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.worker import cutover as cutover_module

    db, conn, stand_in, (s1, s2, s3) = _lane_fixture(tmp_path, monkeypatch)

    clock_calls = {"n": 0}

    def _moving_clock() -> datetime:
        t = _T + timedelta(minutes=20 * clock_calls["n"])
        clock_calls["n"] += 1
        return t

    monkeypatch.setattr(cutover_module, "utc_now", _moving_clock)

    stand_in.raise_once(s1, RuntimeError("synthetic build failure"))

    init_count = {"n": 0}
    real_init = cutover_module.Cutover.__init__

    def _counting_init(self: Any, db_arg: Database) -> None:
        init_count["n"] += 1
        real_init(self, db_arg)

    monkeypatch.setattr(cutover_module.Cutover, "__init__", _counting_init)

    real_advance = cutover_module.Cutover.advance

    async def _wrapped_advance(self: Any) -> Any:
        step = await real_advance(self)
        if step is cutover_module.CutoverStep.ON:
            raise _Stop(step)
        return step

    monkeypatch.setattr(cutover_module.Cutover, "advance", _wrapped_advance)

    with pytest.raises(_Stop):
        asyncio.run(processor_module._run_main_lane(db))

    assert init_count["n"] >= 2, (
        f"M22 -> at this assertion: correct = at least 2 Cutover instances "
        f"(a fresh one after the WAIT), mutant = {init_count['n']}"
    )
    stored = get_runtime_state(conn, DAILY_CLOSE_ON_KEY)
    assert stored is not None
    first_attempt_t = isoformat_utc(_T)
    assert stored != first_attempt_t, (
        f"M22 -> at this assertion: correct = the key holds the fresh "
        f"attempt's own, later T, mutant = {stored!r} (the first, failed "
        f"attempt's Cutover -- and its T -- was kept across the WAIT)"
    )
