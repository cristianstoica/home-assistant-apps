"""The cutover (daily-close switch-over plan §6; test IDs CV1-CV10 of
§17.3).

Written before P6's cutover module exists: every test here imports
``wxverify.worker.cutover`` inside its own body (through ``_cv_fixture`` or
directly), so each fails today at that import (``ModuleNotFoundError``),
never at collection and never on a fixture or typo. CV8 is the exception
(plan §17.1, §19 Step 3): it imports only ``METHODOLOGY_VERSION``, which
already exists, and fails on a plain assertion against today's value (4,
not 5) instead.

The CV fixture (plan §17.3 preamble) is built by ``_cv_fixture``: sites 1
(``UTC``), 2 (``Etc/GMT+5``) and 3 (``UTC``, ``enabled`` = 0); P4's two
seams replaced by the recording stand-in of §17.1. GAP (reported to the
dispatcher): the preamble also calls for "stored pairs for X, Y and Z
before C" on each site; no CV row's expected column reads
``forecast_pairs`` content (only CV5 touches retired *jobs*, not pairs), so
this file omits that part of the fixture rather than building the
generation/feed/pair rows needed for it with no assertion to justify them.

Synthetic data only: no real station id, coordinate, site or zone name.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import pytest

from tests.helpers import asof_make_site
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY, start_site_days

# C, the cutover day (plan §4.1); T = C 10:00Z throughout this file.
_C: Final = date(2025, 9, 1)


def _init_tmp_db(tmp_path: Path) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    return init_db(str(db_path))


def _make_site(conn: sqlite3.Connection, *, timezone: str, enabled: bool = True) -> int:
    site_id = asof_make_site(conn, f"cutover-site-{timezone}-{enabled}")
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


def _cv_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[
    Database, sqlite3.Connection, _PublicationStandIn, tuple[int, int, int], datetime
]:
    import wxverify.worker.cutover as cutover_module

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    conn.execute(
        "CREATE TABLE cutover_test_markers (site_id INTEGER, first_record_date TEXT)"
    )

    site_1 = _make_site(conn, timezone="UTC")
    site_2 = _make_site(conn, timezone="Etc/GMT+5")
    site_3 = _make_site(conn, timezone="UTC", enabled=False)

    t = datetime(_C.year, _C.month, _C.day, 10, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(cutover_module, "utc_now", lambda: t)

    stand_in = _PublicationStandIn()
    monkeypatch.setattr(cutover_module, "build_first_publication", stand_in.build)
    monkeypatch.setattr(cutover_module, "first_publication_ready", stand_in.ready)

    return db, conn, stand_in, (site_1, site_2, site_3), t


async def _advance_to_on(cutover: Any, *, max_steps: int = 20) -> list[Any]:
    from wxverify.worker.cutover import CutoverStep

    steps: list[Any] = []
    for _ in range(max_steps):
        step = await cutover.advance()
        steps.append(step)
        if step is CutoverStep.ON:
            return steps
        if step is CutoverStep.WAIT:
            raise AssertionError(f"advance returned WAIT unexpectedly: {steps}")
    raise AssertionError(f"advance never reached ON within {max_steps} steps: {steps}")


def test_cv1_advance_builds_sites_in_order_then_writes_the_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from wxverify.worker.cutover import Cutover, CutoverStep

    db, conn, stand_in, (s1, s2, s3), t = _cv_fixture(tmp_path, monkeypatch)

    cutover = Cutover(db)
    with caplog.at_level(logging.INFO, logger="wxverify.worker.cutover"):
        steps = asyncio.run(_advance_to_on(cutover))

    assert steps[:3] == [CutoverStep.MORE, CutoverStep.MORE, CutoverStep.MORE], (
        f"M7 -> at this assertion: correct = three MORE steps before ON, "
        f"mutant = {steps[:3]}"
    )
    assert steps[-1] is CutoverStep.ON

    assert [call[0] for call in stand_in.build_calls] == [s1, s2, s3], (
        f"M7 -> at this assertion: correct = sites built in order {[s1, s2, s3]}, "
        f"mutant = {[call[0] for call in stand_in.build_calls]}"
    )

    value = conn.execute(
        "SELECT value FROM runtime_state WHERE key = ?", (DAILY_CLOSE_ON_KEY,)
    ).fetchone()
    assert value is not None

    for site_id in (s1, s2, s3):
        row = conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert row[0] >= 1, (
            f"M7 -> at this assertion (site={site_id}): correct = a first "
            f"record, mutant = none (B2 ran before this site's B1 unit)"
        )

    info_lines = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    for site_id in (s1, s2, s3):
        assert any(
            str(site_id) in line and "daily close on" in line for line in info_lines
        ), f"missing the INFO line naming site {site_id}"


def test_cv2_a_dropped_attempt_resumes_from_where_it_left_off(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.worker.cutover import Cutover, CutoverStep

    db, conn, stand_in, (s1, s2, s3), t = _cv_fixture(tmp_path, monkeypatch)

    for k in range(1, 4):
        cutover = Cutover(db)
        for _ in range(k):
            step = asyncio.run(cutover.advance())
            assert step is CutoverStep.MORE
        # The attempt is dropped here: no more calls on this Cutover.

    final_cutover = Cutover(db)
    steps = asyncio.run(_advance_to_on(final_cutover))
    assert steps[-1] is CutoverStep.ON

    assert len(stand_in.build_calls) == 3, (
        f"M8 -> at this assertion: correct = 3 build calls, one per site, "
        f"mutant = {len(stand_in.build_calls)} (a new attempt skipped or "
        "rebuilt a site an earlier attempt already built)"
    )
    assert {call[0] for call in stand_in.build_calls} == {s1, s2, s3}

    value = conn.execute(
        "SELECT value FROM runtime_state WHERE key = ?", (DAILY_CLOSE_ON_KEY,)
    ).fetchone()
    assert value is not None
    for site_id in (s1, s2, s3):
        row = conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert row[0] >= 1


def test_cv3_a_failed_build_waits_then_a_clean_attempt_reaches_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from wxverify.worker.cutover import Cutover, CutoverStep

    db, conn, stand_in, (s1, s2, s3), t = _cv_fixture(tmp_path, monkeypatch)

    stand_in.raise_once(s2, RuntimeError("synthetic build failure"))

    cutover = Cutover(db)
    with caplog.at_level(logging.ERROR, logger="wxverify.worker.cutover"):
        steps: list[Any] = []
        for _ in range(10):
            step = asyncio.run(cutover.advance())
            steps.append(step)
            if step is not CutoverStep.MORE:
                break

    assert steps[-1] is CutoverStep.WAIT, (
        f"M9 -> at this assertion: correct = WAIT after site 2's failed "
        f"unit, mutant = {steps[-1]}"
    )
    error_lines = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any(str(s2) in line for line in error_lines), (
        "the ERROR line must name the failed site"
    )
    assert get_runtime_state(conn, DAILY_CLOSE_ON_KEY) is None
    row = conn.execute(
        "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (s2,)
    ).fetchone()
    assert row[0] == 0

    new_cutover = Cutover(db)
    final_steps = asyncio.run(_advance_to_on(new_cutover))
    assert final_steps[-1] is CutoverStep.ON
    assert get_runtime_state(conn, DAILY_CLOSE_ON_KEY) is not None


def test_cv4_b2_rereads_each_sites_readiness_before_writing_the_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from wxverify.worker.cutover import Cutover, CutoverStep

    db, conn, stand_in, (s1, s2, s3), t = _cv_fixture(tmp_path, monkeypatch)

    cutover = Cutover(db)
    for _ in range(3):
        step = asyncio.run(cutover.advance())
        assert step is CutoverStep.MORE

    # A test-controlled injection, not a race: site 3's publication is made
    # stale right before B2's own re-check (9.1, 6.4 step 4), isolating that
    # check from whatever B1 itself already verified.
    stand_in._built[s3] = date(2000, 1, 1)  # noqa: SLF001

    with caplog.at_level(logging.ERROR, logger="wxverify.worker.cutover"):
        step = asyncio.run(cutover.advance())

    assert step is CutoverStep.WAIT, (
        f"M10 -> at this assertion: correct = WAIT (B2's ready check caught "
        f"the stale publication), mutant = {step}"
    )
    error_lines = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any(str(s3) in line for line in error_lines)
    assert get_runtime_state(conn, DAILY_CLOSE_ON_KEY) is None
    row = conn.execute(
        "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (s3,)
    ).fetchone()
    assert row[0] == 0


def test_cv5_b2_resettles_retired_jobs_inserted_after_b1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.worker.cutover import Cutover, CutoverStep

    db, conn, stand_in, (s1, s2, s3), t = _cv_fixture(tmp_path, monkeypatch)

    cutover = Cutover(db)
    for _ in range(3):
        step = asyncio.run(cutover.advance())
        assert step is CutoverStep.MORE

    second_conn = sqlite3.connect(str(config.db_path), isolation_level=None)
    try:
        second_conn.execute(
            "INSERT INTO jobs (type, site_id, job_key) VALUES"
            " ('pair_and_score', ?, NULL)",
            (s1,),
        )
        second_conn.execute(
            "INSERT INTO jobs (type, site_id, job_key) VALUES ('fetch_obs', ?, 'obs')",
            (s1,),
        )
    finally:
        second_conn.close()

    step = asyncio.run(cutover.advance())
    assert step is CutoverStep.ON

    remaining = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE site_id = ? AND status IN"
        " ('pending','running')"
        " AND (type = 'pair_and_score'"
        "      OR (type = 'fetch_obs' AND job_key = 'obs'))",
        (s1,),
    ).fetchone()[0]
    assert remaining == 0, (
        f"M11 -> at this assertion: correct = 0 (B2's second settle removed "
        f"the rows a second connection inserted after B1), mutant = {remaining}"
    )


def test_cv6_an_unreadable_zone_at_b2_rolls_back_every_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from wxverify.worker.cutover import Cutover, CutoverStep

    db, conn, stand_in, (s1, s2, s3), t = _cv_fixture(tmp_path, monkeypatch)

    cutover = Cutover(db)
    for _ in range(3):
        step = asyncio.run(cutover.advance())
        assert step is CutoverStep.MORE

    conn.execute("UPDATE sites SET timezone = 'Not/AZone' WHERE id = ?", (s2,))
    conn.commit()

    with caplog.at_level(logging.ERROR, logger="wxverify.worker.cutover"):
        step = asyncio.run(cutover.advance())

    assert step is CutoverStep.WAIT, (
        f"M12 -> at this assertion: correct = WAIT (no site's key or record "
        f"is written), mutant = {step}"
    )
    error_lines = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any(str(s2) in line for line in error_lines)
    assert get_runtime_state(conn, DAILY_CLOSE_ON_KEY) is None
    for site_id in (s1, s2, s3):
        row = conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert row[0] == 0, (
            f"M12 -> at this assertion (site={site_id}): correct = 0 records "
            f"(the whole transaction rolled back), mutant = {row[0]}"
        )


def test_cv7_an_import_between_two_b1_units_waits_then_cuts_over_the_new_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import wxverify.worker.cutover as cutover_module
    from wxverify.worker.cutover import Cutover, CutoverStep

    db, conn, stand_in, (s1, s2, s3), t = _cv_fixture(tmp_path, monkeypatch)

    cutover = Cutover(db)
    step = asyncio.run(cutover.advance())
    assert step is CutoverStep.MORE

    new_path = tmp_path / "replacement.db"
    new_db = Database(str(new_path))
    try:
        _make_site(new_db._conn, timezone="UTC")  # noqa: SLF001
        new_db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        new_db._conn.commit()  # noqa: SLF001
    finally:
        new_db.close()
    backup_path = tmp_path / "backup.db.bak"

    with caplog.at_level(logging.ERROR, logger="wxverify.worker.cutover"):
        asyncio.run(db.replace_from(new_path, backup_path))
        step = asyncio.run(cutover.advance())

    assert step is CutoverStep.WAIT, (
        f"M13 -> at this assertion: correct = WAIT (the stale-generation "
        f"write was rejected, nothing landed on the new file), mutant = {step}"
    )
    live_conn = db._conn  # noqa: SLF001
    assert get_runtime_state(live_conn, DAILY_CLOSE_ON_KEY) is None

    monkeypatch.setattr(cutover_module, "utc_now", lambda: t)
    fresh_cutover = Cutover(db)
    steps = asyncio.run(_advance_to_on(fresh_cutover))
    assert steps[-1] is CutoverStep.ON
    assert get_runtime_state(db._conn, DAILY_CLOSE_ON_KEY) is not None  # noqa: SLF001


def test_cv8_methodology_version_is_5() -> None:
    from wxverify.verification.methodology import METHODOLOGY_VERSION

    assert METHODOLOGY_VERSION == 5, (
        f"M14 -> at this assertion: correct = 5, mutant = {METHODOLOGY_VERSION}"
    )


def test_cv9_a_runtime_error_waits_but_a_cancellation_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from wxverify.worker.cutover import Cutover, CutoverStep

    db, conn, stand_in, (s1, s2, s3), t = _cv_fixture(tmp_path, monkeypatch)

    stand_in.raise_once(s1, RuntimeError("synthetic build failure"))
    cutover = Cutover(db)
    with caplog.at_level(logging.ERROR, logger="wxverify.worker.cutover"):
        step = asyncio.run(cutover.advance())

    assert step is CutoverStep.WAIT, (
        f"M15 -> at this assertion: correct = WAIT for a RuntimeError, mutant = {step}"
    )
    error_lines = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any(str(s1) in line for line in error_lines)

    stand_in.raise_once(s1, asyncio.CancelledError())
    new_cutover = Cutover(db)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(new_cutover.advance())


def test_cv10_b2_with_the_key_already_set_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.worker.cutover import Cutover, CutoverStep

    db, conn, stand_in, (s1, s2, s3), t = _cv_fixture(tmp_path, monkeypatch)

    cutover = Cutover(db)
    for _ in range(3):
        step = asyncio.run(cutover.advance())
        assert step is CutoverStep.MORE

    # Positive control for the row-count oracle used below: the same
    # "COUNT(*) FROM site_days" query, run against a site where a record was
    # actually just created directly, must see it. This proves the oracle
    # can tell "a record exists" from "none does" before we rely on it to
    # read an absence.
    s4 = _make_site(conn, timezone="UTC")
    start_site_days(conn, s4, date(2025, 9, 2), now_utc=t)
    conn.commit()
    s4_rows = conn.execute(
        "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (s4,)
    ).fetchone()[0]
    assert s4_rows >= 1, (
        "setup check: start_site_days must create a row for the oracle below "
        f"to be able to see one; got {s4_rows}"
    )

    # A value distinct from t (what the frozen clock would produce if B2's
    # body ran again), so a mutant that re-runs the body changes it.
    preset_value = isoformat_utc(datetime(2000, 1, 1, 0, 0, 0, tzinfo=UTC))
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, preset_value)
    conn.commit()
    before_value = conn.execute(
        "SELECT value FROM runtime_state WHERE key = ?", (DAILY_CLOSE_ON_KEY,)
    ).fetchone()[0]
    assert before_value == preset_value
    build_calls_before = list(stand_in.build_calls)
    for site_id in (s1, s2, s3):
        row = conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert row[0] == 0, (
            f"setup check: site {site_id} must have no site_days rows before "
            f"the second advance; got {row[0]}"
        )

    step = asyncio.run(cutover.advance())

    assert step is CutoverStep.ON, (
        f"M16 -> at this assertion: correct = ON, mutant = {step}"
    )
    after_value = conn.execute(
        "SELECT value FROM runtime_state WHERE key = ?", (DAILY_CLOSE_ON_KEY,)
    ).fetchone()[0]
    assert after_value == before_value, (
        f"M16 -> at this assertion: correct = {before_value!r} unchanged, "
        f"mutant = {after_value!r} (B2 wrote again although the key was "
        "already set, overwriting it with the frozen clock's own stamp)"
    )
    assert stand_in.build_calls == build_calls_before, (
        "M16 -> at this assertion: correct = no further build call, mutant "
        "= B2 ran its other steps anyway"
    )
    for site_id in (s1, s2, s3):
        row = conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert row[0] == 0, (
            f"M16 -> at this assertion (site={site_id}): correct = 0 site_days "
            f"rows (the key check short-circuited before start_site_days), "
            f"mutant = {row[0]} (start_site_days ran again and created a "
            "first record)"
        )
