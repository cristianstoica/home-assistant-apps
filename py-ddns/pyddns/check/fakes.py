# pyright: strict
"""Recording test doubles for every injectable seam the oracle drives.

Each fake is a *recording* double, not a call-spy mock: the assertions read the
recorded effect (``FakeState.value``, ``FakeHttp.calls``, ``FakeClock`` /
``FakeSleeper`` history), never an expectation set in advance. This mirrors the
py-syslog ``check/fakes.py`` idiom (``CaptureWriter.lines`` /
``RecordingHandler.messages``). Every public fake is imported by at least one
sibling ``check/*.py`` module, so it carries a public name.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from ipaddress import IPv4Address

from ..httpclient import HttpError, HttpResponse
from ..models import ResolveOutcome, ResolveStatus


class FakeState:
    """An in-memory `State`: ``write`` records the last value, ``read`` returns it.

    `writes` records *every* persisted value (not just the last) so a check can
    assert a confirmation gate persisted exactly once, or not at all.
    """

    def __init__(self, initial: IPv4Address | None = None) -> None:
        self.value = initial
        self.writes: list[IPv4Address] = []

    def read(self) -> IPv4Address | None:
        return self.value

    def write(self, value: IPv4Address) -> None:
        self.value = value
        self.writes.append(value)


class FakeIpSource:
    """An `IpSource` returning a fixed (or sequenced) detected IP per cycle."""

    def __init__(self, *results: IPv4Address | None) -> None:
        self._results = list(results) if results else [None]
        self.calls = 0

    def detect(self) -> IPv4Address | None:
        index = min(self.calls, len(self._results) - 1)
        self.calls += 1
        return self._results[index]


class FakeResolver:
    """A `Resolver` returning a scripted sequence of `ResolveOutcome`s.

    Each ``resolve`` call returns the next scripted outcome (the last repeats),
    so a check can stage a pre-fire and a distinct post-fire view of `name`.
    """

    def __init__(self, *outcomes: ResolveOutcome) -> None:
        self._outcomes = (
            list(outcomes)
            if outcomes
            else [ResolveOutcome(ResolveStatus.NO_RECORD, None)]
        )
        self.calls = 0
        self.queried: list[str] = []

    def resolve(self, name: str) -> ResolveOutcome:
        self.queried.append(name)
        index = min(self.calls, len(self._outcomes) - 1)
        self.calls += 1
        return self._outcomes[index]


class FakeProvider:
    """A `DnsProvider` with scripted ``read_current`` / ``apply`` behavior.

    ``read_current`` returns a fixed value (or raises a scripted exception);
    ``apply`` returns a fixed `ApplyResult` (or raises). Records each call.
    """

    def __init__(
        self,
        *,
        read_result: object = None,
        apply_result: object = None,
    ) -> None:
        self._read_result = read_result
        self._apply_result = apply_result
        self.read_calls = 0
        self.apply_calls: list[IPv4Address | None] = []

    def read_current(self) -> object:
        self.read_calls += 1
        if isinstance(self._read_result, BaseException):
            raise self._read_result
        return self._read_result

    def apply(self, detected_ip: IPv4Address | None) -> object:
        self.apply_calls.append(detected_ip)
        if isinstance(self._apply_result, BaseException):
            raise self._apply_result
        return self._apply_result


class FakeClock:
    """A monotonic `Clock` whose value advances only when `advance` is called.

    The oracle drives time explicitly so drift cadence and token expiry are
    deterministic — no wall-clock dependence.
    """

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def __call__(self) -> float:
        return self.now


class FakeSleeper:
    """A synchronous `Sleeper`: records each requested delay, never really sleeps.

    Returns ``True`` (stop signalled) from the call indexed by `stop_at` onward,
    so a check can prove the runner aborts *before its next attempt*. Default
    never stops. This is how the bounded backoff is driven with no thread/Timer.
    """

    def __init__(self, stop_at: int | None = None) -> None:
        self.slept: list[float] = []
        self._stop_at = stop_at

    def __call__(self, seconds: float) -> bool:
        index = len(self.slept)
        self.slept.append(seconds)
        return self._stop_at is not None and index >= self._stop_at


class FakeHttp:
    """A `HttpClient` returning scripted responses/errors keyed by call order.

    `responses` is a list of `HttpResponse` | `HttpError`; each ``request``
    returns/raises the next entry (the last repeats). `calls` records each
    ``(method, url, body)`` so a check can assert the URL/body shaping and the
    request count without a network.
    """

    def __init__(self, *responses: HttpResponse | HttpError) -> None:
        self._responses: list[HttpResponse | HttpError] = list(responses)
        self.calls: list[tuple[str, str, bytes | None]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        data: bytes | None = None,
        timeout: float,
    ) -> HttpResponse:
        self.calls.append((method, url, data))
        if not self._responses:
            raise HttpError("fake http: no scripted response", status=None)
        index = min(len(self.calls) - 1, len(self._responses) - 1)
        entry = self._responses[index]
        if isinstance(entry, HttpError):
            raise entry
        return entry


class RecordingHandler(logging.Handler):
    """Records every message emitted on the ``pyddns`` logger (DEBUG and up).

    The no-secret-leakage and callback-diagnostic checks read ``messages`` to
    assert what was (and was not) logged.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def ok_response(body: str = "", status: int = 200) -> HttpResponse:
    """A 2xx `HttpResponse` convenience for the fakes."""
    return HttpResponse(status, body, {})


def with_recording_handler(run: Callable[[RecordingHandler], None]) -> list[str]:
    """Run `run` with a fresh `RecordingHandler` attached to ``pyddns``; return msgs.

    Restores the prior logger level/handlers, so checks never leak handler state
    into one another.
    """
    logger = logging.getLogger("pyddns")
    handler = RecordingHandler()
    prev_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        run(handler)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)
    return handler.messages
