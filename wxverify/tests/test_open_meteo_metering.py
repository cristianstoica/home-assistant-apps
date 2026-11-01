"""Item A -- Open-Meteo previous-runs metering (A-T1..A-T13).

Pins ``OpenMeteoAdapter.estimate_cost`` (A-T1), ``_metered_calls`` (A-T5) and
the untouched-past-rows guarantee on a second ``run_migrations`` pass (A-T9).
The historical-replay cases (A-T2-A-T4, A-T6-A-T8, A-T10-A-T13) leave with
R10's removal of the previous-runs/historical-replay seam.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import httpx
import pytest

from wxverify import config
from wxverify.db.connection import close_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.feeds.open_meteo import OpenMeteoAdapter, _metered_calls
from wxverify.feeds.seam import ForecastRequest

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

_ALL_VARIABLES = ("temperature", "wind", "precip")


def _req(
    *,
    model: str = "ecmwf_ifs",
    max_lead_hours: int = 168,
    variables: tuple[str, ...] = _ALL_VARIABLES,
) -> ForecastRequest:
    return ForecastRequest(
        lat=0.0,
        lon=0.0,
        model=model,
        variables=variables,
        max_lead_hours=max_lead_hours,
    )


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


# ---------------------------------------------------------------------------
# A-T1 -- forward estimate_cost is unaffected by the horizon
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("max_lead_hours", [168, 384])
def test_forward_estimate_cost_is_always_one_call(max_lead_hours: int) -> None:
    """A-T1: three variables, one deterministic model -- weight 0.3, floored
    to 1 -- regardless of how high Item C later raises the horizon.
    """
    adapter = OpenMeteoAdapter(httpx.AsyncClient())
    req = _req(max_lead_hours=max_lead_hours)
    assert adapter.estimate_cost(req).calls == 1


# ---------------------------------------------------------------------------
# A-T5 -- pure integer arithmetic on _metered_calls
# ---------------------------------------------------------------------------


def test_metered_calls_exact_integer_arithmetic_at_60_billed_days() -> None:
    """A-T5: 21 names * 60 billed days -> ceil(1260 / 140) == 9, computed
    entirely in integers -- independent of how the day count is derived.
    """
    assert _metered_calls(21, 60) == 9


# ---------------------------------------------------------------------------
# A-T9 -- no retroactive adjustment to past api_budget rows
# ---------------------------------------------------------------------------


def test_run_migrations_leaves_past_api_budget_rows_byte_identical(
    tmp_path: Path,
) -> None:
    """A-T9: Item A changes no schema and performs no data migration. Prior
    ``api_budget`` rows are historical fact and are untouched by a second
    ``run_migrations`` pass.
    """
    conn = _init_tmp_db(tmp_path)
    conn.execute(
        "INSERT INTO api_budget (source, billing_day, calls, credits)"
        " VALUES ('open-meteo', '2026-05-01', 42, 0)"
    )
    conn.execute(
        "INSERT INTO api_budget (source, billing_day, calls, credits)"
        " VALUES ('open-meteo', '2026-05-02', 7, 0)"
    )
    conn.commit()
    before = [
        tuple(row)
        for row in conn.execute(
            "SELECT source, billing_day, calls, credits"
            " FROM api_budget ORDER BY billing_day"
        ).fetchall()
    ]
    assert before  # anti-vacuity: the table was not empty going in

    run_migrations(conn)

    after = [
        tuple(row)
        for row in conn.execute(
            "SELECT source, billing_day, calls, credits"
            " FROM api_budget ORDER BY billing_day"
        ).fetchall()
    ]
    assert after == before
