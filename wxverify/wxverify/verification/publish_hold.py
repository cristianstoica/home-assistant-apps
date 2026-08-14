"""Publish-hold transition writer and read model (§7.5, D7, D10).

One module holding both the transition writer and the read model, so the
API route, the Ops context builder, and the migration bootstrap all share
one implementation. `read_publish_hold` derives `held` from
`verification_publish_held` — the scheduler's own function — never from a
parallel copy.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.settings.keys import set_setting

#: `runtime_state` keys recording last-transition metadata only (D7) --
#: never an audit trail, never a previous-state record, never an operator
#: identity.
PUBLISH_HOLD_BOOTSTRAP_KEY = "verification_publish_hold_bootstrap"
PUBLISH_HOLD_LAST_STATE_KEY = "verification_publish_hold_last_state"
PUBLISH_HOLD_LAST_SOURCE_KEY = "verification_publish_hold_last_source"


@dataclass(frozen=True)
class PublishHoldState:
    """The current publish-hold state, as read for the API and web surfaces."""

    held: bool
    last_state: str | None
    last_source: str | None
    last_changed_at: str | None
    bootstrap: str | None
    chain_active: bool


def read_publish_hold(conn: sqlite3.Connection) -> PublishHoldState:
    """Derive the current publish-hold state from its two sources of truth."""
    from wxverify.db.runtime_state import get_runtime_state_entry
    from wxverify.worker.scheduler import verification_publish_held
    from wxverify.worker.verification_run import any_verification_chain_active

    last_state_entry = get_runtime_state_entry(conn, PUBLISH_HOLD_LAST_STATE_KEY)
    last_source = get_runtime_state(conn, PUBLISH_HOLD_LAST_SOURCE_KEY)
    bootstrap = get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY)
    return PublishHoldState(
        held=verification_publish_held(conn),
        last_state=None if last_state_entry is None else last_state_entry.value,
        last_source=last_source,
        last_changed_at=(
            None if last_state_entry is None else last_state_entry.updated_at
        ),
        bootstrap=bootstrap,
        chain_active=any_verification_chain_active(conn),
    )


def set_publish_hold(conn: sqlite3.Connection, *, held: bool, source: str) -> None:
    """Write the hold value and its last-transition metadata (D7).

    `source` is required and keyword-only -- `"bootstrap"` or `"ops"`. Never
    touches the bootstrap marker; that key is written once, by the
    migration bootstrap, and never again (D2).
    """
    from wxverify.worker.scheduler import VERIFICATION_PUBLISH_HOLD_KEY

    set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "1" if held else "0")
    set_runtime_state(conn, PUBLISH_HOLD_LAST_STATE_KEY, "held" if held else "released")
    set_runtime_state(conn, PUBLISH_HOLD_LAST_SOURCE_KEY, source)
