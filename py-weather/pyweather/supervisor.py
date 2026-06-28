# pyright: strict
"""The add-on self-options Supervisor client + the persisted options-blob shaper.

Distinct from the Core-API client (`haapi.HaApiClient`, base
`http://supervisor/core/api`): this targets the Supervisor *add-on* API at
`http://supervisor/addons/self/options` to write the add-on's OWN options. An
add-on may write its own options with no elevated role (the Supervisor
`api_bypass` rule allows an add-on's own token to reach `/addons/self/...` for
everything except `security` and self-`update`); `options` is on the allowed
side. The manifest grants this with `hassio_api: true` (no `hassio_role`).

`to_options_dict` reconstructs the FULL options blob from the running `Config`
(since `config.load` discards the raw options dict), enumerating EVERY current
`config.yaml` schema field explicitly plus the discovered `stations`. The full
blob is sent (not just `stations`) so the POST cannot drop any tuning the
operator had set. The `--check` `persist-allowlist` assertion guards drift
between `to_options_dict` and the co-located `_MANIFEST_OPTION_KEYS` test
constant — it canNOT see `config.yaml` (the runtime never parses it). Both code
constants are a hand-maintained mirror of the `config.yaml` `schema:` block; any
field added to the manifest MUST be added to both by hand (see the CONTRACT
comment in `startup_checks.py`).
"""

from __future__ import annotations

import json
from typing import Protocol

from .errors import TransientError
from .httpclient import HttpClient, HttpError
from .models import Config, Station

BASE_URL = "http://supervisor/addons/self/options"


class SupervisorOptions(Protocol):
    """The persistence seam `_discover_and_persist` / `run_startup` write through.

    The single method the orchestration path calls. The real
    `SupervisorSelfClient` satisfies it (structurally, no explicit inheritance),
    as does the oracle's `FakeSupervisorSelfClient` — so the recording double is
    assignable under pyright-strict without a cast or `# type: ignore`. Typing the
    seam against this Protocol (not the concrete client) is what keeps the
    `--check` fakes type-clean.
    """

    def set_options(self, options: dict[str, object]) -> None: ...


def to_options_dict(cfg: Config, stations: list[Station]) -> dict[str, object]:
    """Build the full self-options POST body from the running `Config`.

    Enumerates every non-`stations` `config.yaml` schema field at its RESOLVED
    value (so an operator-omitted field persists as the in-range default the
    add-on actually ran with) plus the discovered `stations` (each a
    `{key, update_entity}` object). This key set is a
    hand-maintained mirror of the `config.yaml` `schema:` block; the
    `persist-allowlist` check proves it agrees with the co-located
    `_MANIFEST_OPTION_KEYS` test constant (code↔code), NOT with `config.yaml`
    itself — see the CONTRACT comment in `startup_checks.py`.
    """
    return {
        "max_backoff_seconds": cfg.max_backoff_seconds,
        "min_interval_seconds": cfg.min_interval_seconds,
        "settle_seconds": cfg.settle_seconds,
        "startup_stagger_seconds": cfg.startup_stagger_seconds,
        "request_timeout_seconds": cfg.request_timeout_seconds,
        "log_level": cfg.log_level,
        "stations": [
            {
                "key": s.key,
                "update_entity": s.update_entity,
            }
            for s in stations
        ],
    }


class SupervisorSelfClient:
    """POST the add-on's own options to `http://supervisor/addons/self/options`.

    Mirrors `HaApiClient`'s construction (`http`, `token`, `timeout_seconds`) so
    the `--check` oracle drives it against `FakeHttp`. `token` is the same
    `SUPERVISOR_TOKEN` bearer the Core client uses.
    """

    def __init__(self, http: HttpClient, token: str, timeout_seconds: float) -> None:
        self._http = http
        self._token = token
        self._timeout = timeout_seconds

    def set_options(self, options: dict[str, object]) -> None:
        """POST `{"options": <options>}`; raise a single error on failure.

        The body wraps the full options blob under the `options` key (the
        Supervisor `/addons/self/options` contract). On any `HttpError` it raises a
        single `TransientError` carrying THIS endpoint's URL — it does NOT reuse
        `haapi._classify` (whose non-401/403-4xx message is `/states`-specific and
        would mislabel a `POST /addons/self/options` failure). The caller
        (`_discover_and_persist`) does not branch on the error class — it treats any
        persist failure as the best-effort WARNING + paste-block path, never an abort
        — so a single class is correct and the Terminal-vs-Transient distinction is
        irrelevant here.
        """
        body = json.dumps({"options": options}).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        try:
            self._http.request(
                "POST",
                BASE_URL,
                headers=headers,
                data=body,
                timeout=self._timeout,
            )
        except HttpError as exc:
            raise TransientError(f"POST {BASE_URL}: {exc}") from None
