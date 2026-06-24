# pyright: strict
"""Recording test doubles for every injectable seam the oracle drives.

Each fake is a *recording* double, not a call-spy mock: assertions read the
recorded effect (``FakeHttp.calls``, ``FakeSleeper.slept``), never an
expectation set in advance. This mirrors the py-ddns / py-syslog ``check/fakes``
idiom. Every public fake is imported by at least one sibling ``check/*.py``
module, so it carries a public name.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, NamedTuple

from ..httpclient import HttpError, HttpResponse
from ..models import StationCadence


class HttpCall(NamedTuple):
    """One recorded HTTP call: method, url, headers, decoded body, and timeout.

    `body` is the decoded UTF-8 request body (``None`` for a bodyless GET), so a
    check can assert the exact JSON shaping without re-decoding. `headers` is the
    exact dict the seam was called with, so the Authorization/Content-Type
    contract is asserted directly.
    """

    method: str
    url: str
    headers: dict[str, str]
    body: str | None
    timeout: float


class FakeHttp:
    """A `HttpClient` returning scripted responses/errors keyed by call order.

    `responses` is a list of `HttpResponse` | `HttpError`; each ``request``
    returns/raises the next entry (the last repeats). `calls` records each
    `HttpCall` so a check can assert method/url/headers/body/timeout and the call
    count without a network.
    """

    def __init__(self, *responses: HttpResponse | HttpError) -> None:
        self._responses: list[HttpResponse | HttpError] = list(responses)
        self.calls: list[HttpCall] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        data: bytes | None = None,
        timeout: float,
    ) -> HttpResponse:
        self.calls.append(
            HttpCall(
                method=method,
                url=url,
                headers=dict(headers or {}),
                body=data.decode("utf-8") if data is not None else None,
                timeout=timeout,
            )
        )
        if not self._responses:
            raise HttpError("fake http: no scripted response", status=None)
        index = min(len(self.calls) - 1, len(self._responses) - 1)
        entry = self._responses[index]
        if isinstance(entry, HttpError):
            raise entry
        return entry


class FakeClock:
    """A monotonic `Clock` whose value advances only when `advance` is called."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def __call__(self) -> float:
        return self.now


class FakeWallClock:
    """A `WallClock` returning a fixed tz-aware UTC instant (deterministic ``t0``)."""

    def __init__(self, instant: datetime) -> None:
        self._instant = instant

    def now(self) -> datetime:
        return self._instant


class FakeSleeper:
    """A synchronous `Sleeper`: records each requested delay, never really sleeps.

    Returns ``True`` (stop signalled) from the call indexed by `stop_at` onward,
    so a check can prove a runner/loop aborts *before its next read/poll*. Default
    never stops. This is how the settle/re-read waits are driven with no
    thread/Timer.
    """

    def __init__(self, stop_at: int | None = None) -> None:
        self.slept: list[float] = []
        self._stop_at = stop_at

    def __call__(self, seconds: float) -> bool:
        index = len(self.slept)
        self.slept.append(seconds)
        return self._stop_at is not None and index >= self._stop_at


class FakeSave:
    """Records each debounced /data save the scheduler issues (no FS).

    The scheduler hands its live ``dict[str, StationCadence]`` to the save seam
    once per non-raising poll; this snapshots each handed dict (copied so a later
    in-place mutation cannot retroactively rewrite a recorded save) so a check can
    assert the per-cycle save count and the persisted station population.
    """

    def __init__(self) -> None:
        self.saves: list[dict[str, StationCadence]] = []

    def __call__(self, stations: dict[str, StationCadence]) -> None:
        self.saves.append(dict(stations))


class FakeJitter:
    """A deterministic `JitterSource`: ``base * factor`` (identity at ``factor=1.0``).

    Identity (the default) isolates a scheduler interval assertion from jitter, so
    a learned-interval check pins the bare estimator value. A non-identity factor
    (e.g. ``0.85``) proves the scheduler actually routes the interval through the
    injected seam rather than scheduling ``base_interval`` directly.
    """

    def __init__(self, factor: float = 1.0) -> None:
        self._factor = factor

    def __call__(self, base: float) -> float:
        return base * self._factor


def states_response(states: list[dict[str, Any]]) -> HttpResponse:
    """A 2xx `HttpResponse` whose JSON body is the given ``/states`` array."""
    return HttpResponse(200, json.dumps(states), {})


def ok_response(body: str = "", status: int = 200) -> HttpResponse:
    """A 2xx `HttpResponse` convenience for the fakes (e.g. the update_entity POST)."""
    return HttpResponse(status, body, {})
