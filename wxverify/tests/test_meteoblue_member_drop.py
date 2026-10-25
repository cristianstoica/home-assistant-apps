"""Meteoblue member drop oracles — plan Step 6 (§7 option B).

Covers ``wxverify.config.DROPPED_METEOBLUE_MEMBERS``/``DROPPED_METEOBLUE_REASON``
and ``wxverify.db.migrations.pause_dropped_meteoblue_members``: the one-shot,
marker-gated start-up pause of the six dropped meteoblue package members, and
its reach into the served-feed set via ``wxverify.forecast.data.served_feed_ids``.

Fixtures built with ``create_schema`` + a hand-set ``PRAGMA user_version``
(mirrors ``tests/test_google_horizon_correction.py``'s ``_bare_db``), except
the restart test, which opens a real ``Database`` file.

Synthetic data only: the six are real meteoblue model names (the reason this
drop exists at all); the kept member, decoy and package rows are obviously-
fake standins for "some other feed the pause must not touch".
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from wxverify import config
from wxverify.db import migrations
from wxverify.db.connection import Database
from wxverify.db.migrations import (
    METEOBLUE_MEMBER_DROP_KEY,
    TARGET_USER_VERSION,
    create_schema,
    pause_dropped_meteoblue_members,
    run_migrations,
)
from wxverify.db.runtime_state import get_runtime_state
from wxverify.forecast.data import served_feed_ids
from wxverify.web.context import FEED_DESCRIPTIONS


def _bare_db(*, user_version: int) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    conn.execute(f"PRAGMA user_version = {user_version}")
    return conn


def _insert_feed(
    conn: sqlite3.Connection,
    *,
    source: str,
    model: str,
    enabled: bool,
    disabled_reason: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, enabled, disabled_reason,
                           fetch_interval_minutes)
        VALUES (?, ?, ?, ?, 60)
        """,
        (source, model, int(enabled), disabled_reason),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_members_db(*, members_enabled: bool) -> sqlite3.Connection:
    """A v4 database holding the six, a kept member, a decoy and the package.

    The decoy shares a dropped name but not the ``meteoblue`` source; the
    kept member shares the source but not a dropped name; the package row is
    ``('meteoblue', 'multimodel')`` itself. All four stay at ``enabled = 1``
    regardless of the six's state, so each test can see whether the pause
    reached past its intended rows.
    """
    conn = _bare_db(user_version=4)
    for model in config.DROPPED_METEOBLUE_MEMBERS:
        _insert_feed(conn, source="meteoblue", model=model, enabled=members_enabled)
    _insert_feed(conn, source="meteoblue", model="example_member_keep", enabled=True)
    _insert_feed(conn, source="example-src", model="NEMS4", enabled=True)
    _insert_feed(conn, source="meteoblue", model="multimodel", enabled=True)
    return conn


def _feed_state(
    conn: sqlite3.Connection, *, source: str, model: str
) -> tuple[int, str | None]:
    row = conn.execute(
        "SELECT enabled, disabled_reason FROM feeds WHERE source = ? AND model = ?",
        (source, model),
    ).fetchone()
    assert row is not None, f"expected a ({source}, {model}) feed row"
    return (int(row["enabled"]), row["disabled_reason"])


def _user_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version").fetchone()
    return int(row[0])


def test_dropped_members_are_known_meteoblue_models() -> None:
    """Every dropped name is a described meteoblue model, and there are six
    distinct ones, pinned literally so a swap to another kept meteoblue
    model cannot pass unnoticed.

    Catches: a typo or a lower-case name, which would match no stored row;
    a name swapped for another meteoblue model (e.g. a kept model standing
    in for a dropped one), which the count/membership checks alone would
    not catch.
    """
    assert config.DROPPED_METEOBLUE_MEMBERS == (
        "NMM22",
        "NEMSGLOBAL",
        "NEMSGLOBAL_E",
        "NEMS12",
        "NEMS12_E",
        "NEMS4",
    )
    assert len(config.DROPPED_METEOBLUE_MEMBERS) == 6
    assert len(set(config.DROPPED_METEOBLUE_MEMBERS)) == 6
    assert {("meteoblue", m) for m in config.DROPPED_METEOBLUE_MEMBERS} <= set(
        FEED_DESCRIPTIONS
    )


def test_drop_pauses_exactly_the_six() -> None:
    """``run_migrations`` pauses the six and leaves every other row alone.

    The literal reason text below (not ``config.DROPPED_METEOBLUE_REASON``)
    pins the exact string a changed constant would silently drift past.

    Catches: a match without ``source`` (the decoy would be paused too); the
    package row paused; a wrong name list; other reason text.
    """
    conn = _seed_members_db(members_enabled=True)
    run_migrations(conn)

    for model in config.DROPPED_METEOBLUE_MEMBERS:
        enabled, reason = _feed_state(conn, source="meteoblue", model=model)
        assert enabled == 0
        assert reason == "Dropped 2026-10-22: not worth keeping (feed study)"

    kept_enabled, kept_reason = _feed_state(
        conn, source="meteoblue", model="example_member_keep"
    )
    assert kept_enabled == 1
    assert kept_reason is None

    decoy_enabled, decoy_reason = _feed_state(conn, source="example-src", model="NEMS4")
    assert decoy_enabled == 1
    assert decoy_reason is None

    package_enabled, package_reason = _feed_state(
        conn, source="meteoblue", model="multimodel"
    )
    assert package_enabled == 1
    assert package_reason is None

    assert get_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY) == "applied"
    assert _user_version(conn) == TARGET_USER_VERSION


def test_drop_respects_a_later_re_enable() -> None:
    """Once the marker is written, switching a member back on sticks across
    a later run.

    Catches: no marker gate (a second run would pause the re-enabled member
    right back).
    """
    conn = _seed_members_db(members_enabled=True)
    run_migrations(conn)

    one = config.DROPPED_METEOBLUE_MEMBERS[0]
    conn.execute(
        "UPDATE feeds SET enabled = 1, disabled_reason = NULL"
        " WHERE source = 'meteoblue' AND model = ?",
        (one,),
    )
    run_migrations(conn)

    enabled, reason = _feed_state(conn, source="meteoblue", model=one)
    assert enabled == 1
    assert reason is None


def test_drop_re_enable_survives_a_restart(tmp_path: Path) -> None:
    """The same guarantee through real file opens: pause on first open,
    re-enable one member, and the re-enable survives a third open.

    Catches: the gate read from anything that does not persist across opens.
    """
    path = tmp_path / "live.db"

    db = Database(str(path))
    db.close()

    raw = sqlite3.connect(str(path))
    try:
        for model in config.DROPPED_METEOBLUE_MEMBERS:
            raw.execute(
                """
                INSERT INTO feeds (source, model, enabled, fetch_interval_minutes)
                VALUES ('meteoblue', ?, 1, 60)
                """,
                (model,),
            )
        raw.commit()
    finally:
        raw.close()

    db = Database(str(path))
    try:
        rows = db.read_sync(
            lambda conn: conn.execute(
                "SELECT model, enabled FROM feeds WHERE source = 'meteoblue'"
                " AND model IN ({})".format(
                    ", ".join("?" for _ in config.DROPPED_METEOBLUE_MEMBERS)
                ),
                config.DROPPED_METEOBLUE_MEMBERS,
            ).fetchall()
        )
        assert {row["model"]: int(row["enabled"]) for row in rows} == {
            model: 0 for model in config.DROPPED_METEOBLUE_MEMBERS
        }
        assert (
            db.read_sync(
                lambda conn: get_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY)
            )
            == "applied"
        )

        db.write_sync(
            lambda conn: conn.execute(
                "UPDATE feeds SET enabled = 1, disabled_reason = NULL"
                " WHERE source = 'meteoblue' AND model = 'NEMS4'"
            )
        )
    finally:
        db.close()

    db = Database(str(path))
    try:
        rows = db.read_sync(
            lambda conn: conn.execute(
                "SELECT model, enabled FROM feeds WHERE source = 'meteoblue'"
                " AND model IN ({})".format(
                    ", ".join("?" for _ in config.DROPPED_METEOBLUE_MEMBERS)
                ),
                config.DROPPED_METEOBLUE_MEMBERS,
            ).fetchall()
        )
        states = {row["model"]: int(row["enabled"]) for row in rows}
    finally:
        db.close()

    assert states["NEMS4"] == 1
    for model in config.DROPPED_METEOBLUE_MEMBERS:
        if model != "NEMS4":
            assert states[model] == 0


def test_drop_converges_after_a_stop_between_statements() -> None:
    """The state a stop right after the UPDATE but before the marker write
    leaves (the six paused, the reason set, no marker): a run from there is
    a no-op, and the marker lands.

    Catches: a gate that needs rows changed before it writes the marker.
    """
    conn = _seed_members_db(members_enabled=False)
    for model in config.DROPPED_METEOBLUE_MEMBERS:
        conn.execute(
            "UPDATE feeds SET disabled_reason = ?"
            " WHERE source = 'meteoblue' AND model = ?",
            (config.DROPPED_METEOBLUE_REASON, model),
        )
    assert get_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY) is None

    run_migrations(conn)

    for model in config.DROPPED_METEOBLUE_MEMBERS:
        enabled, reason = _feed_state(conn, source="meteoblue", model=model)
        assert enabled == 0
        assert reason == config.DROPPED_METEOBLUE_REASON
    assert get_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY) == "applied"


def test_drop_waits_for_the_members_to_exist() -> None:
    """A database with no members yet writes no marker; once the fetcher's
    registration inserts the six, the next run pauses them and writes it.

    Catches: a marker written on an empty match.
    """
    conn = _bare_db(user_version=4)
    run_migrations(conn)
    assert get_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY) is None

    for model in config.DROPPED_METEOBLUE_MEMBERS:
        conn.execute(
            """
            INSERT INTO feeds (source, model, enabled, fetch_interval_minutes)
            VALUES ('meteoblue', ?, 1, 60)
            """,
            (model,),
        )
    run_migrations(conn)

    for model in config.DROPPED_METEOBLUE_MEMBERS:
        enabled, _ = _feed_state(conn, source="meteoblue", model=model)
        assert enabled == 0
    assert get_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY) == "applied"


def test_drop_with_six_already_off_writes_marker() -> None:
    """The six already off, with a hand-set reason, before any run: the run
    leaves the hand-set reason alone and still writes the marker.

    Catches: a gate on rows changed (the marker would never be written); the
    ``enabled = 1`` filter dropped (the reason would be overwritten).
    """
    conn = _seed_members_db(members_enabled=False)
    for model in config.DROPPED_METEOBLUE_MEMBERS:
        conn.execute(
            "UPDATE feeds SET disabled_reason = 'hand-set'"
            " WHERE source = 'meteoblue' AND model = ?",
            (model,),
        )

    run_migrations(conn)

    for model in config.DROPPED_METEOBLUE_MEMBERS:
        enabled, reason = _feed_state(conn, source="meteoblue", model=model)
        assert enabled == 0
        assert reason == "hand-set"
    assert get_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY) == "applied"


def test_drop_update_precedes_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    """A simulated crash in the marker write: the UPDATE still landed first,
    so the six come out paused with no marker.

    Catches: marker first (a crash there would leave the six on for good).
    """
    conn = _seed_members_db(members_enabled=True)

    def _boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated crash between the UPDATE and the marker")

    monkeypatch.setattr(migrations, "set_runtime_state", _boom)
    with pytest.raises(RuntimeError, match="simulated crash"):
        pause_dropped_meteoblue_members(conn)

    for model in config.DROPPED_METEOBLUE_MEMBERS:
        enabled, reason = _feed_state(conn, source="meteoblue", model=model)
        assert enabled == 0
        assert reason == config.DROPPED_METEOBLUE_REASON
    assert get_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY) is None


def test_drop_never_moves_user_version() -> None:
    """A direct call leaves ``PRAGMA user_version`` exactly where it found
    it: this is a data fix, not a schema migration.

    Catches: a correction that bumps the version to the current target.
    """
    conn = _seed_members_db(members_enabled=True)
    conn.execute("PRAGMA user_version = 99")
    pause_dropped_meteoblue_members(conn)
    assert _user_version(conn) == 99


def _make_site_for_served(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('site-a', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def test_drop_leaves_the_served_set() -> None:
    """A real site's served set drops the six and keeps the kept member,
    once the pause has run.

    The pre-pause check proves the six are served first, so the disjoint
    check is not vacuous.

    Catches: a pause that does not reach the served forecast.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = _make_site_for_served(conn)

    for model in config.DROPPED_METEOBLUE_MEMBERS:
        conn.execute(
            """
            INSERT INTO feeds (source, model, enabled, fetch_interval_minutes)
            VALUES ('meteoblue', ?, 1, 60)
            """,
            (model,),
        )
    keep_id = _insert_feed(
        conn, source="meteoblue", model="example_member_keep", enabled=True
    )
    package_row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'meteoblue' AND model = 'multimodel'"
    ).fetchone()
    assert package_row is not None
    package_id = int(package_row["id"])
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 1)",
        (site_id, package_id),
    )
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 1)",
        (site_id, keep_id),
    )
    conn.commit()

    member_ids = set()
    for model in config.DROPPED_METEOBLUE_MEMBERS:
        row = conn.execute(
            "SELECT id FROM feeds WHERE source = 'meteoblue' AND model = ?", (model,)
        ).fetchone()
        assert row is not None
        member_ids.add(int(row["id"]))

    assert member_ids <= served_feed_ids(conn, site_id=site_id)
    assert keep_id in served_feed_ids(conn, site_id=site_id)

    pause_dropped_meteoblue_members(conn)
    conn.commit()

    served = served_feed_ids(conn, site_id=site_id)
    assert keep_id in served
    assert served.isdisjoint(member_ids)
