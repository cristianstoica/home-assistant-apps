"""Runtime settings domain writers."""

from __future__ import annotations

import sqlite3

from wxverify.core.options import RuntimeOptions, depth_option_values
from wxverify.db.connection import get_db
from wxverify.settings.depth import depth_override_key
from wxverify.settings.keys import set_setting


async def set_rolling_window_days(days: int) -> None:
    def _write(conn: sqlite3.Connection) -> None:
        set_rolling_window_days_sync(conn, days)

    await get_db().write(_write)


def set_rolling_window_days_sync(conn: sqlite3.Connection, days: int) -> None:
    if days < 1 or days > 3650:
        raise ValueError("rolling_window_days out of range")
    set_setting(conn, "rolling_window_days", str(days))
    # Single-branch contract, run on every apply (no change detection): keep
    # `w:all` and the CURRENT `w:{days}` slice, drop obsolete `w:N` keys. An
    # unchanged boot re-apply preserves the usable rolling cache across
    # restarts; a genuine value change invalidates the previous slice and the
    # next rescore rebuilds the new one.
    conn.execute(
        "DELETE FROM score_cache WHERE window_key LIKE 'w:%' "
        "AND window_key NOT IN ('w:all', ?)",
        (f"w:{days}",),
    )


async def apply_plain_settings(options: RuntimeOptions) -> None:
    def _apply(conn: sqlite3.Connection) -> None:
        if options.min_n is not None:
            set_setting(conn, "min_n", str(options.min_n))
        if options.forecast_blend_depth is not None:
            set_setting(conn, "forecast_blend_depth", str(options.forecast_blend_depth))
        # §15 clearing rule — scoped to the three per-variable depth keys
        # only: a key absent from this apply DELETES its settings row, so
        # the global fallback takes effect and provenance reads "global".
        # The plain keys above keep their apply-when-present semantics.
        for variable, value in depth_option_values(options).items():
            key = depth_override_key(variable)
            if value is not None:
                set_setting(conn, key, str(int(value)))
            else:
                conn.execute("DELETE FROM settings WHERE key = ?", (key,))
        if options.obs_interval_minutes is not None:
            set_setting(conn, "obs_interval_minutes", str(options.obs_interval_minutes))
        if options.obs_jitter_minutes is not None:
            set_setting(conn, "obs_jitter_minutes", str(options.obs_jitter_minutes))
        if options.min_interval_seconds is not None:
            set_setting(conn, "min_interval_seconds", str(options.min_interval_seconds))
        if options.max_backoff_seconds is not None:
            set_setting(conn, "max_backoff_seconds", str(options.max_backoff_seconds))
        if options.request_timeout_seconds is not None:
            set_setting(
                conn, "request_timeout_seconds", str(options.request_timeout_seconds)
            )

    await get_db().write(_apply)
