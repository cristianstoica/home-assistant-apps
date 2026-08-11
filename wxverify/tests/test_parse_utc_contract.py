"""parse_utc's exception contract: any unparseable str raises ValueError.

``datetime.fromisoformat`` raises only ``ValueError``, but a syntactically
valid ISO-8601 stamp at the year-1/year-9999 boundary carrying a non-UTC
offset is pushed out of ``datetime``'s representable range by the
``.astimezone(UTC)`` conversion, which raises ``OverflowError``. Call sites
across the tree catch ``ValueError`` and nothing wider, so the helper must
normalize that carrier to ``ValueError`` at the boundary -- for any ``str``
input, ``parse_utc`` either returns a UTC datetime or raises ``ValueError``,
never anything else. The caller-level tests here prove the contract is
load-bearing where an escaping exception would degrade or halt a route.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import parse_utc
from wxverify.db.connection import close_db, get_db
from wxverify.obs.pws_adapter import _obs_datetime

# Well-formed ISO-8601 stamps whose non-UTC offset pushes the UTC conversion
# outside datetime's representable range: fromisoformat accepts them, and it
# is the .astimezone(UTC) step that overflows.
LOW_BOUNDARY_OFFSET_STAMP = "0001-01-01T00:00:00+05:00"
HIGH_BOUNDARY_OFFSET_STAMP = "9999-12-31T23:59:59-05:00"


def test_low_boundary_offset_stamp_raises_value_error() -> None:
    with pytest.raises(ValueError, match="out of range"):
        parse_utc(LOW_BOUNDARY_OFFSET_STAMP)


def test_high_boundary_offset_stamp_raises_value_error() -> None:
    with pytest.raises(ValueError, match="out of range"):
        parse_utc(HIGH_BOUNDARY_OFFSET_STAMP)


@pytest.mark.parametrize(
    "value",
    [
        LOW_BOUNDARY_OFFSET_STAMP,
        HIGH_BOUNDARY_OFFSET_STAMP,
        "zzzz",
        "",
        "9999-not-a-timestamp",
    ],
    ids=[
        "low_boundary_offset",
        "high_boundary_offset",
        "garbage",
        "empty",
        "year_like_garbage",
    ],
)
def test_unparseable_strings_raise_value_error_and_nothing_else(value: str) -> None:
    """The contract is total: no exception type other than ValueError may
    escape for a str input. pytest.raises(ValueError) does not catch
    OverflowError, so a regression here fails loudly, not silently."""
    with pytest.raises(ValueError):
        parse_utc(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-01-02T03:04:05Z", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
        ("2026-01-02T03:04:05+02:00", datetime(2026, 1, 2, 1, 4, 5, tzinfo=UTC)),
        ("2026-01-02T03:04:05", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)),
    ],
    ids=["zulu", "positive_offset", "naive_assumed_utc"],
)
def test_valid_inputs_still_parse_exactly(value: str, expected: datetime) -> None:
    """Negative control: the overflow normalization must not wrap so much
    that a valid conversion turns into a ValueError or changes value."""
    assert parse_utc(value) == expected


def test_obs_datetime_returns_none_for_boundary_offset_stamp() -> None:
    """The observation adapter's timestamp reader catches ValueError and maps
    an unusable upstream stamp to None; a boundary-offset carrier from a
    hostile payload must take that path, not escape the fetch."""
    assert _obs_datetime({"obsTimeUtc": HIGH_BOUNDARY_OFFSET_STAMP}) is None


async def _idle_worker_async(db: object) -> None:  # keep the real worker idle
    await asyncio.Event().wait()


def test_monitor_route_survives_boundary_offset_worker_started_at(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A boundary-offset worker_started_at must degrade to grace-inactive
    inside _grace_active, leaving every pipeline condition reportable --
    not escape build_verdict's narrow sqlite3.Error catch and collapse the
    whole /api/health/monitor response to the outer-guard error verdict."""
    close_db()
    config.db_path = str(tmp_path / "monitor-boundary-grace.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker_async)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT OR REPLACE INTO runtime_state(key, value)"
                " VALUES ('worker_started_at', ?)",
                (LOW_BOUNDARY_OFFSET_STAMP,),
            )

        db.write_sync(_seed)
        resp = client.get("/api/health/monitor")
        assert resp.status_code == 200
        body = resp.json()
        assert body["grace_active"] is False
        ids = {c["id"] for c in body["conditions"]}
        assert "unexpected_error" not in ids
        for cond_id in (
            "feed_stale",
            "obs_stale",
            "fetch_obs_live",
            "fetch_feed_live",
            "pair_score_live",
            "problem_jobs",
        ):
            assert cond_id in ids
