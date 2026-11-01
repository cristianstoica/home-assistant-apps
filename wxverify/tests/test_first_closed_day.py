"""The first closed day (daily-close switch-over plan §4; test IDs FD1-FD5
of §17.2).

Written before P6's cutover module exists: every test here imports
``wxverify.worker.cutover`` inside its own body, so the whole file collects
today and each test fails at that import (``ModuleNotFoundError``), never at
collection time and never on a fixture or typo.

Synthetic data only: zones ``UTC``, ``Etc/GMT+5`` and ``Etc/GMT-14`` (no
daylight saving, so a day is always 24 hours, plan §17.1); stations
``ISTATION01``-``ISTATION06`` where a site needs one.
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
from wxverify.db.connection import Database, close_db, init_db

# C, the cutover day used by every row below (plan §4.1).
_C: Final = date(2025, 6, 10)


def _init_tmp_db(tmp_path: Path) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    return init_db(str(db_path))


def _make_site(conn: sqlite3.Connection, *, timezone: str, enabled: bool = True) -> int:
    site_id = asof_make_site(conn, f"switch-site-{timezone}")
    conn.execute(
        "UPDATE sites SET timezone = ?, enabled = ? WHERE id = ?",
        (timezone, 1 if enabled else 0, site_id),
    )
    return site_id


class _PublicationStandIn:
    """Records every call to P4's two seams (plan §17.1) and writes one
    marker row per site when it "builds" a publication, instead of running
    P4's real build. Can be told to raise, once or every time, for a site.
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


def test_fd1_at_or_after_2300_local_gives_c_plus_1_both_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from wxverify.worker.cutover import first_closed_day

    at_2300 = datetime(_C.year, _C.month, _C.day, 23, 0, 0, tzinfo=UTC)
    at_235959 = datetime(_C.year, _C.month, _C.day, 23, 59, 59, tzinfo=UTC)
    expected = _C + timedelta(days=1)

    result_2300 = first_closed_day("UTC", at_2300)
    result_235959 = first_closed_day("UTC", at_235959)

    assert result_2300 == expected, (
        f"M2 -> at this assertion: correct = {expected.isoformat()}, "
        f"mutant = {(expected + timedelta(days=1)).isoformat()}"
    )
    assert result_235959 == expected, (
        f"M2 -> at this assertion: correct = {expected.isoformat()}, "
        f"mutant = {(expected + timedelta(days=1)).isoformat()}"
    )


def test_fd2_before_2300_local_still_gives_c_plus_1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from wxverify.worker.cutover import first_closed_day

    expected = _C + timedelta(days=1)
    times = (
        datetime(_C.year, _C.month, _C.day, 0, 0, 0, tzinfo=UTC),
        datetime(_C.year, _C.month, _C.day, 9, 0, 0, tzinfo=UTC),
        datetime(_C.year, _C.month, _C.day, 22, 59, 59, tzinfo=UTC),
    )

    for now_utc in times:
        result = first_closed_day("UTC", now_utc)
        assert result == expected, (
            f"M1 -> at this assertion (now_utc={now_utc.isoformat()}): "
            f"correct = {expected.isoformat()}, mutant = {_C.isoformat()}"
        )


def test_fd3_gmt_minus_14_crosses_two_local_midnights(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from wxverify.worker.cutover import first_closed_day

    tz = "Etc/GMT-14"
    before = datetime(_C.year, _C.month, _C.day, 8, 59, 59, tzinfo=UTC)
    at_local_2300 = datetime(_C.year, _C.month, _C.day, 9, 0, 0, tzinfo=UTC)
    at_local_midnight = datetime(_C.year, _C.month, _C.day, 10, 0, 0, tzinfo=UTC)

    result_before = first_closed_day(tz, before)
    result_at_2300 = first_closed_day(tz, at_local_2300)
    result_at_midnight = first_closed_day(tz, at_local_midnight)

    expected_c_plus_1 = _C + timedelta(days=1)
    expected_c_plus_2 = _C + timedelta(days=2)

    assert result_before == expected_c_plus_1, (
        f"M1 -> at this assertion: correct = {expected_c_plus_1.isoformat()}, "
        f"mutant = {_C.isoformat()}"
    )
    assert result_at_2300 == expected_c_plus_1, (
        f"M2 -> at this assertion: correct = {expected_c_plus_1.isoformat()}, "
        f"mutant = {expected_c_plus_2.isoformat()}"
    )
    assert result_at_midnight == expected_c_plus_2, (
        f"M3 -> at this assertion: correct = {expected_c_plus_2.isoformat()}, "
        f"mutant = {expected_c_plus_1.isoformat()} (UTC date, not local, plus one)"
    )


def test_fd4_gmt_plus_5_stays_on_the_earlier_local_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from wxverify.worker.cutover import first_closed_day

    tz = "Etc/GMT+5"
    c_plus_1 = _C + timedelta(days=1)
    now_utc = datetime(c_plus_1.year, c_plus_1.month, c_plus_1.day, 3, 30, tzinfo=UTC)

    result = first_closed_day(tz, now_utc)

    expected = _C + timedelta(days=1)
    wrong_from_utc_date = _C + timedelta(days=2)
    assert result == expected, (
        f"M3 -> at this assertion: correct = {expected.isoformat()}, "
        f"mutant = {wrong_from_utc_date.isoformat()}"
    )
    assert result == expected, (
        f"M4 -> at this assertion: correct = {expected.isoformat()}, "
        f"mutant = {wrong_from_utc_date.isoformat()}"
    )


def test_fd5_advance_seeds_each_site_from_its_own_local_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import wxverify.worker.cutover as cutover_module
    from wxverify.worker.cutover import Cutover

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    conn.execute(
        "CREATE TABLE cutover_test_markers (site_id INTEGER, first_record_date TEXT)"
    )

    site_utc = _make_site(conn, timezone="UTC")
    site_gmt5 = _make_site(conn, timezone="Etc/GMT+5", enabled=False)
    site_gmt14 = _make_site(conn, timezone="Etc/GMT-14")

    t = datetime(_C.year, _C.month, _C.day, 10, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(cutover_module, "utc_now", lambda: t)

    stand_in = _PublicationStandIn()
    monkeypatch.setattr(cutover_module, "build_first_publication", stand_in.build)
    monkeypatch.setattr(cutover_module, "first_publication_ready", stand_in.ready)

    cutover = Cutover(db)
    asyncio.run(_advance_to_on(cutover))

    expected_f = {
        site_utc: _C + timedelta(days=1),
        site_gmt5: _C + timedelta(days=1),
        site_gmt14: _C + timedelta(days=2),
    }
    for site_id, f in expected_f.items():
        row = conn.execute(
            "SELECT MIN(local_date) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert row is not None and row[0] is not None, (
            f"M5 -> at this assertion (site={site_id}): correct = a first record, "
            "mutant = none (B2 skipped a disabled site)"
        )
        assert row[0] == f.isoformat(), (
            f"M6 -> at this assertion (site={site_id}): correct = {f.isoformat()}, "
            f"mutant = {(f - timedelta(days=1)).isoformat()}"
        )

    # One ensure_site_days call at T adds nothing before each site's F.
    from wxverify.db.site_days import ensure_site_days

    for site_id in expected_f:
        ensure_site_days(conn, site_id, now_utc=t)
    for site_id, f in expected_f.items():
        row = conn.execute(
            "SELECT MIN(local_date) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert row is not None and row[0] == f.isoformat(), (
            f"at this assertion (site={site_id}): correct = {f.isoformat()} still "
            f"the earliest record, mutant = an earlier record appeared"
        )
