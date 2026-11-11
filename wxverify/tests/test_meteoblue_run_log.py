"""Tests for ``wxverify.worker.run_log`` -- card 27 step 1, one meteoblue
run-log INFO line per model per reply (0.16.20 plan §2).

Every expected literal below is derived directly from the plan's §2.3 line
format and §2.2 contract, never recomputed by calling the code under test.

Convention: ``httpx.MockTransport`` with a synchronous handler, matching
``tests/test_meteoblue_wind_unit_regression.py``. All fixture data is
synthetic: site ``Testsite`` at ``0.0, 0.0`` in zone ``UTC``, API key
``"test-key"``, made-up models ``MODEL_A``/``MODEL_B``, 2030-dated stamps.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.feeds.meteoblue import MeteoblueAdapter, _to_fetch_result
from wxverify.feeds.seam import FetchResult, ForecastRequest, ModelRun
from wxverify.worker.feed_fetch import DownloadSlot, FetchFeedSuccess, fetch_feed_once
from wxverify.worker.run_log import run_lines

_RUN_LOG_LOGGER = "wxverify.worker.run_log"


def _run_log_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == _RUN_LOG_LOGGER]


# ---------------------------------------------------------------------------
# T10 -- run_lines, pure
# ---------------------------------------------------------------------------


def test_run_lines_prev_new_updated_and_model_name_tokens() -> None:
    """T10: exact strings for prev later/equal/earlier, none, unread, the
    updated token (a stamp, ``none``, ``unreadable-array``), and a model
    name holding a space and a newline.

    M11 -> at the ``EQUAL`` line below: correct = "...prev=2030-06-01T00:00:00Z
    new=no", mutant (``>=`` in place of ``>``) = "...new=yes" -- the run and
    the stored prev are equal, which is exactly the case ``>`` and ``>=``
    disagree on.
    M12 -> at the ``bad_name_`` line below: correct = the model token reads
    "bad_name_" (space and newline each replaced by ``_``), mutant (name not
    cleaned) = the raw "bad name\\n" embedded verbatim, which breaks the
    "no spaces inside a value" shape this line's exact-string comparison
    pins.
    """
    runs = (
        ModelRun(
            model="LATER", run_at="2030-06-02T00:00:00Z", updated="2030-06-02T05:00:00Z"
        ),
        ModelRun(model="EQUAL", run_at="2030-06-01T00:00:00Z", updated="none"),
        ModelRun(
            model="EARLIER", run_at="2030-05-01T00:00:00Z", updated="unreadable-array"
        ),
        ModelRun(model="ABSENT", run_at="2030-06-01T00:00:00Z", updated="none"),
        ModelRun(model="PRUNED", run_at="2030-06-01T00:00:00Z", updated="none"),
        ModelRun(model="GARBLED", run_at="2030-06-01T00:00:00Z", updated="none"),
        ModelRun(model="bad name\n", run_at="2030-06-01T00:00:00Z", updated="none"),
    )
    stored = {
        "LATER": "LATER:2030-06-01T00:00:00Z",
        "EQUAL": "EQUAL:2030-06-01T00:00:00Z",
        "EARLIER": "EARLIER:2030-06-01T00:00:00Z",
        # "ABSENT" deliberately has no key: a model new to the reply.
        "PRUNED": None,  # the feed exists but every row was pruned.
        "GARBLED": "GARBLED:not-a-timestamp",  # right prefix, unparseable stamp.
        # "bad name\n" deliberately has no key either.
    }

    lines = run_lines(
        source="meteoblue",
        call_at="2030-06-01T00:00:00Z",
        call_local="2030-06-01T00:00:00",
        site_id=7,
        runs=runs,
        stored=stored,
    )

    assert lines == [
        "meteoblue run: call=2030-06-01T00:00:00Z site=7 model=LATER"
        " run=2030-06-02T00:00:00Z updated=2030-06-02T05:00:00Z"
        " prev=2030-06-01T00:00:00Z new=yes local=2030-06-01T00:00:00",
        "meteoblue run: call=2030-06-01T00:00:00Z site=7 model=EQUAL"
        " run=2030-06-01T00:00:00Z updated=none"
        " prev=2030-06-01T00:00:00Z new=no local=2030-06-01T00:00:00",
        "meteoblue run: call=2030-06-01T00:00:00Z site=7 model=EARLIER"
        " run=2030-05-01T00:00:00Z updated=unreadable-array"
        " prev=2030-06-01T00:00:00Z new=no local=2030-06-01T00:00:00",
        "meteoblue run: call=2030-06-01T00:00:00Z site=7 model=ABSENT"
        " run=2030-06-01T00:00:00Z updated=none prev=none new=unknown"
        " local=2030-06-01T00:00:00",
        "meteoblue run: call=2030-06-01T00:00:00Z site=7 model=PRUNED"
        " run=2030-06-01T00:00:00Z updated=none prev=none new=unknown"
        " local=2030-06-01T00:00:00",
        "meteoblue run: call=2030-06-01T00:00:00Z site=7 model=GARBLED"
        " run=2030-06-01T00:00:00Z updated=none prev=unread new=unknown"
        " local=2030-06-01T00:00:00",
        "meteoblue run: call=2030-06-01T00:00:00Z site=7 model=bad_name_"
        " run=2030-06-01T00:00:00Z updated=none prev=none new=unknown"
        " local=2030-06-01T00:00:00",
    ]


def test_run_lines_stored_read_failure_marks_every_prev_unread() -> None:
    """T10: ``stored=None`` (the whole read failed) -- every line reads
    ``prev=unread new=unknown``, regardless of how its own run compares to
    anything; paired with the positive above where a readable ``stored``
    gives ``prev=none``/``prev=<stamp>``, so this failure path is not the
    same branch as "model absent"."""
    runs = (
        ModelRun(model="X", run_at="2030-06-01T00:00:00Z", updated="none"),
        ModelRun(
            model="Y", run_at="2030-06-02T00:00:00Z", updated="2030-06-02T01:00:00Z"
        ),
    )

    lines = run_lines(
        source="meteoblue",
        call_at="2030-06-01T00:00:00Z",
        call_local="2030-06-01T00:00:00",
        site_id=7,
        runs=runs,
        stored=None,
    )

    assert lines == [
        "meteoblue run: call=2030-06-01T00:00:00Z site=7 model=X"
        " run=2030-06-01T00:00:00Z updated=none prev=unread new=unknown"
        " local=2030-06-01T00:00:00",
        "meteoblue run: call=2030-06-01T00:00:00Z site=7 model=Y"
        " run=2030-06-02T00:00:00Z updated=2030-06-02T01:00:00Z"
        " prev=unread new=unknown local=2030-06-01T00:00:00",
    ]


# ---------------------------------------------------------------------------
# T11 -- meteoblue _to_fetch_result's runs field
# ---------------------------------------------------------------------------


def _mb_payload(
    models: list[str],
    *,
    updatetime: object = "__absent__",
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "models": models,
        "modelrun_utc": ["2030-06-01 00:00" for _ in models],
        "latitude": 0.0,
        "longitude": 0.0,
    }
    if updatetime != "__absent__":
        metadata["modelrun_updatetime_utc"] = updatetime
    return {
        "metadata": metadata,
        "multimodel": {
            "data_1h": {
                "time": ["2030-06-01 01:00"],
                "temperature": [[10.0 + i] for i in range(len(models))],
            }
        },
    }


def _fetch_result_for(payload: dict[str, object]) -> FetchResult:
    from wxverify.feeds.meteoblue import MeteoblueResponse  # local: test-only import

    req = ForecastRequest(
        lat=0.0,
        lon=0.0,
        model="multimodel",
        variables=("temperature",),
        max_lead_hours=168,
    )
    return _to_fetch_result(req, MeteoblueResponse.model_validate(payload))


def test_to_fetch_result_runs_field_absent_gives_none_for_every_model() -> None:
    """T11: no ``modelrun_updatetime_utc`` key at all -> every run's
    ``updated`` is ``"none"``; never raises."""
    result = _fetch_result_for(_mb_payload(["A", "B"]))
    assert [r.updated for r in result.runs] == ["none", "none"]
    assert [r.model for r in result.runs] == ["A", "B"]


def test_to_fetch_result_runs_field_list_as_long_as_models_reads_per_index() -> None:
    """T11: a list the same length as ``models`` reads item ``i`` for model
    ``i``; ``None`` in the list gives ``none`` for that one model only."""
    result = _fetch_result_for(
        _mb_payload(["A", "B"], updatetime=["2030-06-02 05:00", None])
    )
    assert [r.updated for r in result.runs] == ["2030-06-02T05:00:00Z", "none"]


def test_to_fetch_result_runs_field_single_string_applies_to_every_model() -> None:
    """T11: a plain ``str`` (not a list) applies the same parsed stamp to
    every model."""
    result = _fetch_result_for(_mb_payload(["A", "B"], updatetime="2030-06-02 05:00"))
    assert [r.updated for r in result.runs] == [
        "2030-06-02T05:00:00Z",
        "2030-06-02T05:00:00Z",
    ]


def test_to_fetch_result_runs_field_wrong_length_list_is_unreadable_array() -> None:
    """T11: a list a different length than ``models`` is ``unreadable-array``
    for every model, read without ever indexing past the list -- never
    raises and never reads another model's time.

    M13 -> at this assertion: correct = ["unreadable-array",
    "unreadable-array"] (two models, a one-item list -- the mismatch), the
    one real update time never attributed to model B; mutant (the list read
    without a length check) = either an ``IndexError`` (this assertion never
    runs, the test errors) or ``["2030-06-02T05:00:00Z",
    "2030-06-02T05:00:00Z"]`` (B silently reads A's update time).
    """
    result = _fetch_result_for(_mb_payload(["A", "B"], updatetime=["2030-06-02 05:00"]))
    assert [r.updated for r in result.runs] == ["unreadable-array", "unreadable-array"]


def test_to_fetch_result_runs_field_list_item_number_is_unreadable_number() -> None:
    """T11: a list item that is a number (not ``None``/``str``) is
    ``unreadable-<kind>`` for that model only -- ``unreadable-number``."""
    result = _fetch_result_for(_mb_payload(["A", "B"], updatetime=[42, None]))
    assert [r.updated for r in result.runs] == ["unreadable-number", "none"]


def test_to_fetch_result_runs_field_unparseable_string_is_unreadable_text() -> None:
    """T11: a string ``parse_utc`` cannot read gives ``unreadable-text`` for
    every model, never raises."""
    result = _fetch_result_for(_mb_payload(["A", "B"], updatetime="not-a-date"))
    assert [r.updated for r in result.runs] == ["unreadable-text", "unreadable-text"]


def test_to_fetch_result_runs_field_object_is_unreadable_object() -> None:
    """T11: a JSON object (not ``None``/``str``/``list``) is
    ``unreadable-object`` for every model."""
    result = _fetch_result_for(_mb_payload(["A", "B"], updatetime={"nested": True}))
    assert [r.updated for r in result.runs] == [
        "unreadable-object",
        "unreadable-object",
    ]


def test_to_fetch_result_run_at_equals_samples_issued_at_per_model() -> None:
    """T11: each run's ``run_at`` equals the canonical ``issued_at`` the
    samples of that same model carry."""
    result = _fetch_result_for(_mb_payload(["A", "B"]))
    by_model = {r.model: r.run_at for r in result.runs}
    for sample in result.samples:
        assert sample.issued_at == by_model[sample.model]
    assert set(by_model) == {"A", "B"}


# ---------------------------------------------------------------------------
# T12/T13/T13b -- fetch_feed_once driving a real meteoblue reply
# ---------------------------------------------------------------------------

_CALL_AT_DT = datetime(2030, 6, 10, 12, 0, 0, tzinfo=UTC)
_CALL_AT_STR = "2030-06-10T12:00:00Z"


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _insert_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
        ).lastrowid
    )


def _meteoblue_package_feed_id(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='meteoblue' AND model='multimodel'"
            " AND is_virtual=0 ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )


def _subscribe(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )


def _register_model_a_with_prior_run(
    conn: sqlite3.Connection, site_id: int, run_at: str
) -> None:
    """Plants a member feed for ``MODEL_A`` with one stored sample at
    ``run_at`` -- the shape of a model that has been saved before."""
    feed_id = int(
        conn.execute(
            "INSERT INTO feeds (source, model, enabled, default_subscribed,"
            " fetch_interval_minutes, max_lead_hours, is_virtual)"
            " VALUES ('meteoblue', 'MODEL_A', 1, 0, 360, 168, 0)"
        ).lastrowid
    )
    conn.execute(
        "INSERT INTO forecast_samples (site_id, feed_id, variable, issued_at,"
        " valid_at, lead_hours, value, source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, 'temperature', ?, ?, 24, 5.0, '5.0 C', ?, ?)",
        (site_id, feed_id, run_at, run_at, f"MODEL_A:{run_at}", isoformat_utc()),
    )
    conn.commit()


def _mb_reply_payload(run_at_raw: str, valid_at_raw: str) -> dict[str, object]:
    return {
        "metadata": {
            "models": ["MODEL_A", "MODEL_B"],
            "modelrun_utc": [run_at_raw, run_at_raw],
            "latitude": 0.0,
            "longitude": 0.0,
        },
        "multimodel": {
            "data_1h": {
                "time": [valid_at_raw],
                "temperature": [[10.0], [12.0]],
            }
        },
    }


def _fetch_once(
    db: object, site_id: int, feed_id: int, payload: dict[str, object]
) -> tuple[object, httpx.AsyncClient]:
    def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        return httpx.Response(200, json=payload)

    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def _build(source: str, client: httpx.AsyncClient) -> MeteoblueAdapter:  # noqa: ARG001
        return MeteoblueAdapter("test-key", mock_client)

    try:
        outcome = asyncio.run(
            fetch_feed_once(
                db,
                site_id,
                feed_id,
                adapter_builder=_build,
                slot=DownloadSlot(
                    slot="unscheduled",
                    round_date=None,
                    round_try=None,
                    started_at=_CALL_AT_DT,
                ),
            )
        )
    finally:
        asyncio.run(mock_client.aclose())
    return outcome, mock_client


def test_fetch_feed_once_meteoblue_reply_logs_one_line_per_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T12: MODEL_A has a prior stored run R0, MODEL_B has none; the reply
    carries both at R1 > R0. Lines: A ``prev=R0 new=yes``, B
    ``prev=none new=unknown``. The same reply again: both ``new=no``. No
    record carries the key, the host or the site's coordinate query params.

    M14 -> at the first-reply assertion below: correct = A reads
    ``prev=2030-06-01T00:00:00Z new=yes`` (the log runs before the persist
    write, so ``prev`` is what was saved BEFORE this reply), mutant (the log
    call moved after the persist write) = A reads ``prev=2030-06-05T00:00:00Z
    new=no`` -- this reply's own just-saved run read back as "already there".
    """
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: _CALL_AT_DT)
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _meteoblue_package_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    r0 = "2030-06-01T00:00:00Z"
    r1 = "2030-06-05T00:00:00Z"
    _register_model_a_with_prior_run(conn, site_id, r0)
    db = get_db()

    caplog.set_level(logging.INFO, logger=_RUN_LOG_LOGGER)
    caplog.clear()
    outcome, _ = _fetch_once(
        db, site_id, feed_id, _mb_reply_payload("2030-06-05 00:00", "2030-06-05 01:00")
    )
    assert outcome == FetchFeedSuccess(inserted=2, usable=2)

    records = _run_log_records(caplog)
    messages = [r.getMessage() for r in records]
    assert messages == [
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_A"
        f" run={r1} updated=none prev={r0} new=yes"
        " local=2030-06-10T12:00:00",
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_B"
        f" run={r1} updated=none prev=none new=unknown"
        " local=2030-06-10T12:00:00",
    ]
    for message in messages:
        assert "test-key" not in message
        assert "my.meteoblue.com" not in message
        assert "lat=0.0" not in message
        assert "lon=0.0" not in message

    caplog.clear()
    outcome2, _ = _fetch_once(
        db, site_id, feed_id, _mb_reply_payload("2030-06-05 00:00", "2030-06-05 01:00")
    )
    assert outcome2 == FetchFeedSuccess(inserted=0, usable=2)
    repeat_messages = [r.getMessage() for r in _run_log_records(caplog)]
    assert repeat_messages == [
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_A"
        f" run={r1} updated=none prev={r1} new=no local=2030-06-10T12:00:00",
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_B"
        f" run={r1} updated=none prev={r1} new=no local=2030-06-10T12:00:00",
    ]


def test_fetch_feed_once_meteoblue_stored_read_failure_is_contained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T13: ``STORED_RUNS_SQL`` patched to a statement that raises
    ``sqlite3.OperationalError`` -- the fetch still returns
    ``FetchFeedSuccess`` with its samples stored, one WARNING
    "meteoblue run log: stored runs not read (OperationalError)", and lines
    with ``prev=unread new=unknown``.

    M15 -> at this assertion: correct = exactly one WARNING reading
    "meteoblue run log: stored runs not read (OperationalError)" plus two
    INFO lines with ``prev=unread``; mutant (the inner try/except removed)
    = the outer handler catches it instead: one WARNING reading "meteoblue
    run log: not written (OperationalError)", and NO "meteoblue run:" INFO
    line at all -- both literals this test reads differ from the mutant's.
    """
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: _CALL_AT_DT)
    monkeypatch.setattr(
        "wxverify.worker.run_log.STORED_RUNS_SQL",
        "SELECT * FROM feeds_that_do_not_exist_xyz",
    )
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _meteoblue_package_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    db = get_db()

    caplog.set_level(logging.INFO, logger=_RUN_LOG_LOGGER)
    caplog.clear()
    outcome, _ = _fetch_once(
        db, site_id, feed_id, _mb_reply_payload("2030-06-05 00:00", "2030-06-05 01:00")
    )
    assert outcome == FetchFeedSuccess(inserted=2, usable=2)

    records = _run_log_records(caplog)
    warnings = [r for r in records if r.levelno == logging.WARNING]
    infos = [r for r in records if r.levelno == logging.INFO]
    assert [r.getMessage() for r in warnings] == [
        "meteoblue run log: stored runs not read (OperationalError)"
    ]
    assert [r.getMessage() for r in infos] == [
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_A"
        " run=2030-06-05T00:00:00Z updated=none prev=unread new=unknown"
        " local=2030-06-10T12:00:00",
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_B"
        " run=2030-06-05T00:00:00Z updated=none prev=unread new=unknown"
        " local=2030-06-10T12:00:00",
    ]


def test_fetch_feed_once_meteoblue_run_log_write_failure_is_contained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T13b: ``run_lines`` patched to raise ``RuntimeError("synthetic")`` --
    never fails the fetch, outside the read. ``fetch_feed_once`` returns
    ``FetchFeedSuccess``; the samples and the ``stored`` download row are
    saved; exactly one WARNING on logger ``wxverify.worker.run_log``,
    "meteoblue run log: not written (RuntimeError)"; no "meteoblue run:"
    line.

    M15b -> at this assertion: correct = ``fetch_feed_once`` returns
    ``FetchFeedSuccess`` (the outer try/except in ``log_model_runs``
    contains the ``RuntimeError``); mutant (the outer try/except removed)
    = ``fetch_feed_once`` itself raises ``RuntimeError``, so this assertion
    never runs -- the test fails at the ``fetch_feed_once`` call, not at
    a value mismatch.
    """
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: _CALL_AT_DT)
    monkeypatch.setattr(
        "wxverify.worker.run_log.run_lines",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("synthetic")),  # noqa: ARG005
    )
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _meteoblue_package_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    db = get_db()

    caplog.set_level(logging.INFO, logger=_RUN_LOG_LOGGER)
    caplog.clear()
    outcome, _ = _fetch_once(
        db, site_id, feed_id, _mb_reply_payload("2030-06-05 00:00", "2030-06-05 01:00")
    )
    assert outcome == FetchFeedSuccess(inserted=2, usable=2)

    records = _run_log_records(caplog)
    assert [r.getMessage() for r in records] == [
        "meteoblue run log: not written (RuntimeError)"
    ]
    assert all(r.levelno == logging.WARNING for r in records)

    samples = conn.execute("SELECT COUNT(*) AS n FROM forecast_samples").fetchone()
    assert samples["n"] == 2
    download = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_downloads WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    ).fetchone()
    assert download["n"] == 1


# ---------------------------------------------------------------------------
# New -- the line's ``local`` field (0.16.20 plan's meteoblue-times release)
# ---------------------------------------------------------------------------


def test_fetch_feed_once_meteoblue_run_log_local_time_for_a_fixed_offset_zone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """New: a site on the fixed +14 offset zone ``tests/test_round_
    schedule.py`` uses for its RS7 case (``Etc/GMT-14``). At
    ``_CALL_AT_STR`` (2030-06-10T12:00:00Z), the line's ``local`` field
    reads 2030-06-11T02:00:00 -- the same instant 14 hours ahead.
    """
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: _CALL_AT_DT)
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'Etc/GMT-14')"
        ).lastrowid
    )
    feed_id = _meteoblue_package_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    db = get_db()

    caplog.set_level(logging.INFO, logger=_RUN_LOG_LOGGER)
    caplog.clear()
    outcome, _ = _fetch_once(
        db, site_id, feed_id, _mb_reply_payload("2030-06-05 00:00", "2030-06-05 01:00")
    )
    assert outcome == FetchFeedSuccess(inserted=2, usable=2)

    messages = [r.getMessage() for r in _run_log_records(caplog)]
    assert messages == [
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_A"
        " run=2030-06-05T00:00:00Z updated=none prev=none new=unknown"
        " local=2030-06-11T02:00:00",
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_B"
        " run=2030-06-05T00:00:00Z updated=none prev=none new=unknown"
        " local=2030-06-11T02:00:00",
    ]


def test_fetch_feed_once_meteoblue_run_log_unreadable_zone_falls_back_to_unread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """New: a site whose stored ``timezone`` does not load -- ``local``
    reads ``unread``, the usual lines are still written, and there is
    exactly one WARNING naming the failure's type. Paired with the
    positive case above: that one shows ``local`` resolving to a real
    offset when the zone loads, so this one is not a blanket default.
    """
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: _CALL_AT_DT)
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'Not/AZone')"
        ).lastrowid
    )
    feed_id = _meteoblue_package_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    db = get_db()

    caplog.set_level(logging.INFO, logger=_RUN_LOG_LOGGER)
    caplog.clear()
    outcome, _ = _fetch_once(
        db, site_id, feed_id, _mb_reply_payload("2030-06-05 00:00", "2030-06-05 01:00")
    )
    assert outcome == FetchFeedSuccess(inserted=2, usable=2)

    records = _run_log_records(caplog)
    warnings = [r for r in records if r.levelno == logging.WARNING]
    infos = [r for r in records if r.levelno == logging.INFO]
    assert [r.getMessage() for r in warnings] == [
        "meteoblue run log: site time not read (ZoneInfoNotFoundError)"
    ]
    assert [r.getMessage() for r in infos] == [
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_A"
        " run=2030-06-05T00:00:00Z updated=none prev=none new=unknown"
        " local=unread",
        f"meteoblue run: call={_CALL_AT_STR} site={site_id} model=MODEL_B"
        " run=2030-06-05T00:00:00Z updated=none prev=none new=unknown"
        " local=unread",
    ]
