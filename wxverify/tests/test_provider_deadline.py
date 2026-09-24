"""Provider-call deadline pins (§6.3.9 of the station-health-and-retries plan).

httpx timeouts apply per operation, not to the whole call: a body that
delivers each chunk before the read timeout expires can keep the call open
indefinitely. ``_get_with_deadline`` wraps every weather.com ``client.get``
in a total ``asyncio.timeout`` of ``read_seconds + PROVIDER_DEADLINE_MARGIN_SECONDS``,
converting ONLY its own expiry to ``ProviderDeadlineExceeded`` -- a transport
``TimeoutError`` and an external cancel both pass through unchanged.

Covers DL1 (deadline fires, all four call sites) and DL2 (only the
deadline's own expiry is converted). DL3-DL5 exercise the shared call lock
and are tested with it.

Convention: ``httpx.MockTransport`` + ``asyncio.run(run())``, matching
``test_pws_adapter_params.py``. New file (rather than folding into that one)
because the fixture here -- a forever-trickling ``httpx.AsyncByteStream`` --
and the ``asyncio.timeout`` hang guard are specific to deadline behaviour and
unrelated to that file's request-param regression pins.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Final
from unittest.mock import patch

import httpx
import pytest

from wxverify.obs.pws_adapter import (
    PROVIDER_DEADLINE_MARGIN_SECONDS,
    ProviderDeadlineExceeded,
    _get_with_deadline,  # noqa: PLC2701 -- exercising the helper directly for DL2
    fetch_current_observation,
    fetch_hourly_history,
    fetch_hourly_history_range,
    validate_station,
)

# ---------------------------------------------------------------------------
# Test constants -- synthetic placeholder data only
# ---------------------------------------------------------------------------

_STATION_ID = "ISTATION01"
_API_KEY = "ci-placeholder"
_CURRENT_URL = "https://api.weather.com/v2/pws/observations/current"

# Each test's outer hang guard: if the deadline machinery is broken, a test
# should fail fast with a clear timeout rather than hang the suite.
_HANG_GUARD_SECONDS = 2

# The trickle byte interval must stay comfortably below every case's read
# timeout so the read timeout itself never fires -- only the total deadline
# under test should.
_TRICKLE_INTERVAL_SECONDS = 0.05

# The deadline every DL1 case is patched to hit.
_TEST_DEADLINE_SECONDS = 0.3


class _TricklingByteStream(httpx.AsyncByteStream):
    """Yields one byte every ``_TRICKLE_INTERVAL_SECONDS``, forever.

    Never completes on its own, so the only way a read of this stream ends is
    the deadline (or an external cancel) cutting it off. Records whether
    ``aclose`` ran and signals ``started`` after the first byte, so a test can
    wait for the read to be genuinely in flight before acting on it.
    """

    def __init__(self) -> None:
        self.closed = False
        self.started: asyncio.Event = asyncio.Event()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            self.started.set()
            yield b"x"
            await asyncio.sleep(_TRICKLE_INTERVAL_SECONDS)

    async def aclose(self) -> None:
        self.closed = True


def _trickling_transport() -> tuple[httpx.MockTransport, _TricklingByteStream]:
    stream = _TricklingByteStream()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    return httpx.MockTransport(handler), stream


# ---------------------------------------------------------------------------
# DL1 -- the deadline fires, at all four call sites
# ---------------------------------------------------------------------------

# name -> that function's read timeout, so the per-test margin patch lands
# every case on the same _TEST_DEADLINE_SECONDS total deadline (margin =
# _TEST_DEADLINE_SECONDS - read_seconds; the helper reads the module
# constant at call time, not at import time).
_DL1_READ_SECONDS: Final[dict[str, float]] = {
    "validate_station": 10.0,
    "fetch_hourly_history": 10.0,
    "fetch_hourly_history_range": 20.0,
    "fetch_current_observation": 10.0,
}


class TestDeadlineFires:
    """DL1: parametrized over the four weather.com call sites.

    Mutant: a raw ``client.get`` in place of ``_get_with_deadline`` at any
    call site. With no total deadline, the trickling body never yields
    ``ProviderDeadlineExceeded`` and the outer hang guard fires instead --
    the ``pytest.raises`` context in the test below never observes the
    right exception type and the test fails (differently, per site).
    """

    @pytest.mark.parametrize(
        "name", sorted(_DL1_READ_SECONDS), ids=sorted(_DL1_READ_SECONDS)
    )
    def test_deadline_exceeded_closes_stream_and_omits_secrets(self, name: str) -> None:
        read_seconds = _DL1_READ_SECONDS[name]
        margin = _TEST_DEADLINE_SECONDS - read_seconds

        async def run() -> None:
            async with asyncio.timeout(_HANG_GUARD_SECONDS):
                transport, stream = _trickling_transport()
                with patch(
                    "wxverify.obs.pws_adapter.PROVIDER_DEADLINE_MARGIN_SECONDS",
                    margin,
                ):
                    if name == "validate_station":
                        real_async_client = httpx.AsyncClient

                        def factory(
                            *args: object, **kwargs: object
                        ) -> httpx.AsyncClient:
                            return real_async_client(transport=transport)

                        with (
                            patch(
                                "wxverify.obs.pws_adapter.httpx.AsyncClient",
                                new=factory,
                            ),
                            pytest.raises(ProviderDeadlineExceeded) as exc_info,
                        ):
                            # lat/lon omitted: this must reach the network,
                            # not take the coordinates-supplied short circuit.
                            await validate_station(_STATION_ID, _API_KEY)
                    else:
                        async with httpx.AsyncClient(transport=transport) as client:
                            with pytest.raises(ProviderDeadlineExceeded) as exc_info:
                                if name == "fetch_hourly_history":
                                    await fetch_hourly_history(
                                        _STATION_ID, _API_KEY, client=client
                                    )
                                elif name == "fetch_hourly_history_range":
                                    await fetch_hourly_history_range(
                                        _STATION_ID,
                                        _API_KEY,
                                        window_start="2026-07-10T00:00:00Z",
                                        window_end="2026-07-10T12:00:00Z",
                                        client=client,
                                    )
                                else:
                                    await fetch_current_observation(
                                        _STATION_ID,
                                        _API_KEY,
                                        client=client,
                                        timeout_seconds=read_seconds,
                                    )

                assert stream.closed, "aclose must run when the deadline expires"
                message = str(exc_info.value)
                assert _API_KEY not in message, "message must not carry the key"
                assert _STATION_ID not in message, (
                    "message must not carry the station id"
                )

        asyncio.run(run())


# ---------------------------------------------------------------------------
# DL2 -- only the deadline's own expiry is converted
# ---------------------------------------------------------------------------


class TestOnlyDeadlineExpiryIsConverted:
    """DL2: an external cancel and a transport-raised TimeoutError both pass
    through the helper unchanged; only the total-deadline's own expiry
    becomes ``ProviderDeadlineExceeded``.

    Mutant: drop the ``cm.expired()`` check (always wrap). The transport
    ``TimeoutError`` case below then comes back as ``ProviderDeadlineExceeded``
    (a ``TimeoutError`` subclass), so ``type(exc) is TimeoutError`` fails.
    """

    def test_external_cancel_stays_cancelled_error(self) -> None:
        async def run() -> None:
            async with asyncio.timeout(_HANG_GUARD_SECONDS):
                transport, stream = _trickling_transport()
                async with httpx.AsyncClient(transport=transport) as client:
                    task = asyncio.create_task(
                        _get_with_deadline(
                            client,
                            _CURRENT_URL,
                            params={"stationId": _STATION_ID, "apiKey": _API_KEY},
                            read_seconds=10.0,
                        )
                    )
                    # Let the read actually start (at least one byte yielded)
                    # before cancelling, so the cancel lands mid-call rather
                    # than before the request is even sent.
                    await stream.started.wait()
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                assert stream.closed, "aclose must run on an external cancel too"

        asyncio.run(run())

    def test_transport_timeout_error_passes_through_unchanged(self) -> None:
        async def run() -> None:
            async with asyncio.timeout(_HANG_GUARD_SECONDS):

                def handler(request: httpx.Request) -> httpx.Response:
                    raise TimeoutError("synthetic transport timeout")

                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)
                ) as client:
                    with pytest.raises(TimeoutError) as exc_info:
                        await _get_with_deadline(
                            client,
                            _CURRENT_URL,
                            params={"stationId": _STATION_ID, "apiKey": _API_KEY},
                            read_seconds=10.0,
                        )
                assert type(exc_info.value) is TimeoutError, (
                    "a transport-raised TimeoutError must pass through "
                    f"unwrapped, not become {type(exc_info.value).__name__}"
                )

        asyncio.run(run())


def test_margin_constant_is_five_seconds_by_default() -> None:
    """Sanity pin on the shipped default, so a change to it is visible here too."""
    assert PROVIDER_DEADLINE_MARGIN_SECONDS == 5.0
