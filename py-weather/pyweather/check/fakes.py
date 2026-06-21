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
import random
from datetime import datetime
from typing import Any, NamedTuple

from ..httpclient import HttpError, HttpResponse


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


class SequenceRandom(random.Random):
    """A `random.Random` returning scripted `randint` values in order.

    Subclasses `random.Random` so it satisfies the `Scheduler` type directly;
    only `randint` is overridden (the sole randomness the scheduler uses). Each
    call returns the next scripted value (the last repeats), so a check can pin
    the exact healthy interval the reward path schedules.
    """

    def __init__(self, *values: int) -> None:
        super().__init__()
        self._values = list(values) if values else [350]
        self.calls = 0

    def randint(self, a: int, b: int) -> int:  # noqa: ARG002 - scripted, ignores bounds
        index = min(self.calls, len(self._values) - 1)
        self.calls += 1
        return self._values[index]


def states_response(states: list[dict[str, Any]]) -> HttpResponse:
    """A 2xx `HttpResponse` whose JSON body is the given ``/states`` array."""
    return HttpResponse(200, json.dumps(states), {})


def ok_response(body: str = "", status: int = 200) -> HttpResponse:
    """A 2xx `HttpResponse` convenience for the fakes (e.g. the update_entity POST)."""
    return HttpResponse(status, body, {})
