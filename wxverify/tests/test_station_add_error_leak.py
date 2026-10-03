"""Station-add provider failures never leak the key, station id or coords.

Regression suite for #108 (plan: 2026-10-03-station-add-error-leak.md §5).
Drives the real ``stations.create_station`` route through a real
``uvicorn.Server`` (not Starlette's ``TestClient``, which never runs
uvicorn's own exception-logging path) with ``httpx.AsyncClient`` replaced by
a factory that injects an ``httpx.MockTransport`` -- the leak sink is the
HTTP layer's own request URL, so the fake sits at that seam, not at the
provider call boundary. Every expected answer text and WARNING line is a
literal in this file, never imported from ``stations.py``.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
import ssl
import threading
import time
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import NoReturn
from urllib.parse import urlsplit

import httpcore
import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from starlette.requests import Request

from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.errors import ApiError
from wxverify.api.routes import stations
from wxverify.api.schemas import StationCreate
from wxverify.core.log_redaction import RedactUrlSecretsFilter
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.obs.pws_adapter import ProviderDeadlineExceeded, PwsStation
from wxverify.worker.control import JobDeferred
from wxverify.worker.station_pacing import weathercom_call_lock

KEY = "ci-placeholder"
STATION_ID = "KTEST001"
LAT = 12.345678
LON = -123.456789

SYNTHETIC_TEXT = (
    "synthetic failure https://api.weather.com/v2/pws/observations/current"
    "?stationId=KTEST001&apiKey=ci-placeholder "
    "https://api.open-meteo.com/v1/elevation"
    "?latitude=12.345678&longitude=-123.456789"
)

NEEDLES = (
    "ci-placeholder",
    "KTEST001",
    "12.345678",
    "123.456789",
    "api.weather.com/v2/pws/observations/current?",
    "api.open-meteo.com/v1/elevation?",
)

DEBUG_ALLOWLIST = (
    re.compile(
        r"wxverify\.obs\.pws_adapter DEBUG pws validate_station station=KTEST001"
    ),
    re.compile(
        r"httpx INFO HTTP Request: GET "
        r"https://api\.weather\.com/v2/pws/observations/current\?"
        r"stationId=KTEST001&format=json&units=m&apiKey=%2A%2A%2A "
        r'"HTTP/1\.1 \d{3} [A-Za-z ]*"'
    ),
    re.compile(
        r"httpx INFO HTTP Request: GET "
        r"https://api\.open-meteo\.com/v1/elevation\?"
        r"latitude=12\.345678&longitude=-123\.456789 "
        r'"HTTP/1\.1 \d{3} [A-Za-z ]*"'
    ),
)


# ---------------------------------------------------------------------------
# Provider response / failure builders
# ---------------------------------------------------------------------------


def _status(
    status: int,
    *,
    json_body: object | None = None,
    content: bytes | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        if json_body is not None:
            return httpx.Response(status, json=json_body)
        return httpx.Response(status, content=b"" if content is None else content)

    return handler


def _raise_httpx(
    exc_type: type[httpx.HTTPError],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc_type(SYNTHETIC_TEXT, request=request)

    return handler


def _raise_deadline(request: httpx.Request) -> httpx.Response:
    raise ProviderDeadlineExceeded(SYNTHETIC_TEXT)


def _raise_tls_connect_error(request: httpx.Request) -> NoReturn:
    """Reproduce the real TLS-connect exception chain shape deterministically."""
    try:
        try:
            raise ssl.SSLCertVerificationError(1, SYNTHETIC_TEXT)
        except ssl.SSLError as tls_exc:
            core = httpcore.ConnectError(SYNTHETIC_TEXT)
            try:
                raise core from tls_exc
            except httpcore.ConnectError:
                raise core from None
    except httpcore.ConnectError as hidden:
        raise httpx.ConnectError(SYNTHETIC_TEXT, request=request) from hidden


def _make_router(
    wc: Callable[[httpx.Request], httpx.Response] | None,
    om: Callable[[httpx.Request], httpx.Response] | None,
    requests: list[tuple[str, str, dict[str, str]]],
) -> Callable[[httpx.Request], httpx.Response]:
    def router(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        requests.append((host, request.url.path, dict(request.url.params)))
        if host == "api.weather.com":
            if wc is None:
                return httpx.Response(
                    200, json={"observations": [{"lat": LAT, "lon": LON}]}
                )
            return wc(request)
        if host == "api.open-meteo.com":
            if om is None:
                return httpx.Response(200, json={"elevation": [1234.5]})
            return om(request)
        return httpx.Response(418)

    return router


def _make_db_fail_stub(
    calls: list[int],
) -> Callable[[sqlite3.Connection, httpx.Response], str | None]:
    def stub(conn: sqlite3.Connection, response: httpx.Response) -> str | None:
        calls.append(1)
        raise sqlite3.OperationalError("synthetic backoff write failure")

    return stub


# ---------------------------------------------------------------------------
# Expected answer texts (literals, never imported from stations.py)
# ---------------------------------------------------------------------------


def _wc_key_rejected(status: int) -> str:
    return (
        f"weather.com rejected the API key (HTTP {status}); station was not "
        "added. Check the weather.com API key in the add-on configuration."
    )


def _wc_unknown_station() -> str:
    return (
        "weather.com has no station with this ID, or the station is not "
        "reporting; station was not added. Check the station ID."
    )


def _wc_refused(status: int) -> str:
    return f"weather.com refused the request (HTTP {status}); station was not added."


def _wc_unavailable(status: int) -> str:
    return (
        f"weather.com is unavailable (HTTP {status}); station was not added. "
        "Try again later."
    )


def _wc_timeout() -> str:
    return (
        "weather.com did not answer in time; station was not added. Try again shortly."
    )


def _wc_unreachable() -> str:
    return "Could not reach weather.com; station was not added. Try again shortly."


def _wc_unreadable() -> str:
    return "weather.com sent a response that could not be read; station was not added."


def _om_refused(status: int) -> str:
    return (
        f"Open-Meteo refused the elevation lookup (HTTP {status}); "
        "station was not added."
    )


def _om_unavailable(status: int) -> str:
    return (
        f"Open-Meteo is unavailable (HTTP {status}); station was not added. "
        "Try again later."
    )


def _om_timeout() -> str:
    return (
        "Open-Meteo did not answer the elevation lookup in time; station was "
        "not added. Try again shortly."
    )


def _om_unreachable() -> str:
    return (
        "Could not reach Open-Meteo for the elevation lookup; station was not "
        "added. Try again shortly."
    )


def _om_unreadable() -> str:
    return (
        "Open-Meteo sent an elevation response that could not be read; "
        "station was not added."
    )


def _warning_pattern(
    provider: str,
    reason: str,
    status: int | None,
    error_type: str,
    *,
    kind: str = "-",
    cause: str = "-",
    where: tuple[str, str] | None = None,
) -> str:
    status_s = "-" if status is None else str(status)
    prefix = (
        f"station add failed provider={provider} reason={reason} "
        f"status={status_s} error_type={error_type} kind={kind} cause={cause} where="
    )
    if where is None:
        return re.escape(prefix + "-")
    file_name, func_name = where
    return re.escape(f"{prefix}{file_name}:") + r"\d+" + re.escape(f" in {func_name}")


@dataclass(frozen=True)
class FCase:
    case_id: str
    wc: Callable[[httpx.Request], httpx.Response] | None
    om: Callable[[httpx.Request], httpx.Response] | None
    http_status: int
    text: str
    warning_pattern: str
    w_count: int
    o_count: int
    backoff_stub: bool = False


FCASES: tuple[FCase, ...] = (
    FCase(
        "W401",
        _status(401),
        None,
        502,
        _wc_key_rejected(401),
        _warning_pattern("weathercom", "key_rejected", 401, "HTTPStatusError"),
        1,
        0,
    ),
    FCase(
        "W403",
        _status(403),
        None,
        502,
        _wc_key_rejected(403),
        _warning_pattern("weathercom", "key_rejected", 403, "HTTPStatusError"),
        1,
        0,
    ),
    FCase(
        "W404",
        _status(404),
        None,
        422,
        _wc_unknown_station(),
        _warning_pattern("weathercom", "unknown_station", 404, "HTTPStatusError"),
        1,
        0,
    ),
    FCase(
        "W400",
        _status(400),
        None,
        502,
        _wc_refused(400),
        _warning_pattern("weathercom", "refused", 400, "HTTPStatusError"),
        1,
        0,
    ),
    FCase(
        "W204",
        _status(204),
        None,
        422,
        _wc_unknown_station(),
        _warning_pattern(
            "weathercom",
            "unknown_station",
            204,
            "UpstreamPayloadError",
            kind="no_content",
        ),
        1,
        0,
    ),
    FCase(
        "W200-empty",
        _status(200, content=b""),
        None,
        422,
        _wc_unknown_station(),
        _warning_pattern(
            "weathercom",
            "unknown_station",
            200,
            "UpstreamPayloadError",
            kind="json_decode",
            cause="JSONDecodeError<-StopIteration",
        ),
        1,
        0,
    ),
    FCase(
        "W200-text",
        _status(200, content=b"not json"),
        None,
        502,
        _wc_unreadable(),
        _warning_pattern(
            "weathercom",
            "unreadable",
            200,
            "UpstreamPayloadError",
            kind="json_decode",
            cause="JSONDecodeError<-StopIteration",
            where=("pws_adapter.py", "decode_observations_payload"),
        ),
        1,
        0,
    ),
    FCase(
        "W-noobs",
        _status(200, json_body={"observations": []}),
        None,
        502,
        _wc_unreadable(),
        _warning_pattern(
            "weathercom",
            "unreadable",
            None,
            "RuntimeError",
            where=("pws_adapter.py", "validate_station"),
        ),
        1,
        0,
    ),
    FCase(
        "W-badlat",
        _status(200, json_body={"observations": [{"lat": "x", "lon": 0.0}]}),
        None,
        502,
        _wc_unreadable(),
        _warning_pattern(
            "weathercom",
            "unreadable",
            None,
            "ValueError",
            where=("pws_adapter.py", "validate_station"),
        ),
        1,
        0,
    ),
    FCase(
        "W-connect",
        _raise_tls_connect_error,
        None,
        502,
        _wc_unreachable(),
        _warning_pattern(
            "weathercom",
            "unreachable",
            None,
            "ConnectError",
            cause="ConnectError<-SSLCertVerificationError",
        ),
        1,
        0,
    ),
    FCase(
        "W-readtimeout",
        _raise_httpx(httpx.ReadTimeout),
        None,
        504,
        _wc_timeout(),
        _warning_pattern("weathercom", "timeout", None, "ReadTimeout"),
        1,
        0,
    ),
    FCase(
        "W-deadline",
        _raise_deadline,
        None,
        504,
        _wc_timeout(),
        _warning_pattern("weathercom", "timeout", None, "ProviderDeadlineExceeded"),
        1,
        0,
    ),
    FCase(
        "W503-nobackoff",
        _status(503),
        None,
        502,
        _wc_unavailable(503),
        _warning_pattern("weathercom", "unavailable", 503, "HTTPStatusError"),
        1,
        0,
        backoff_stub=True,
    ),
    FCase(
        "O401",
        None,
        _status(401),
        502,
        _om_refused(401),
        _warning_pattern("open-meteo", "refused", 401, "HTTPStatusError"),
        1,
        1,
    ),
    FCase(
        "O403",
        None,
        _status(403),
        502,
        _om_refused(403),
        _warning_pattern("open-meteo", "refused", 403, "HTTPStatusError"),
        1,
        1,
    ),
    FCase(
        "O404",
        None,
        _status(404),
        502,
        _om_refused(404),
        _warning_pattern("open-meteo", "refused", 404, "HTTPStatusError"),
        1,
        1,
    ),
    FCase(
        "O-badvalue",
        None,
        _status(200, json_body={"elevation": ["x"]}),
        502,
        _om_unreadable(),
        _warning_pattern(
            "open-meteo",
            "unreadable",
            None,
            "ValueError",
            where=("elevation.py", "lookup_elevation_m"),
        ),
        1,
        1,
    ),
    FCase(
        "O-text",
        None,
        _status(200, content=b"not json"),
        502,
        _om_unreadable(),
        _warning_pattern(
            "open-meteo",
            "unreadable",
            None,
            "JSONDecodeError",
            cause="StopIteration",
            where=("elevation.py", "lookup_elevation_m"),
        ),
        1,
        1,
    ),
    FCase(
        "O-connect",
        None,
        _raise_httpx(httpx.ConnectError),
        502,
        _om_unreachable(),
        _warning_pattern("open-meteo", "unreachable", None, "ConnectError"),
        1,
        1,
    ),
    FCase(
        "O-readtimeout",
        None,
        _raise_httpx(httpx.ReadTimeout),
        504,
        _om_timeout(),
        _warning_pattern("open-meteo", "timeout", None, "ReadTimeout"),
        1,
        1,
    ),
    FCase(
        "O503-nobackoff",
        None,
        _status(503),
        502,
        _om_unavailable(503),
        _warning_pattern("open-meteo", "unavailable", 503, "HTTPStatusError"),
        1,
        1,
        backoff_stub=True,
    ),
)

# Cases whose single provider call raises before httpx logs a completed
# response (a transport error or a timeout) -- the DEBUG "HTTP Request:"
# line is never emitted for that call, even though the request was made.
_WC_RAISES_IDS = frozenset({"W-connect", "W-readtimeout", "W-deadline"})
_OM_RAISES_IDS = frozenset({"O-connect", "O-readtimeout"})


# ---------------------------------------------------------------------------
# Logger normalisation + capture
# ---------------------------------------------------------------------------


class _ListHandler(logging.Handler):
    def __init__(self, fmt: logging.Formatter) -> None:
        super().__init__()
        self.setFormatter(fmt)
        self.records: list[logging.LogRecord] = []
        self.formatted: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.formatted.append(self.format(record))
        self.records.append(record)


@dataclass
class _LoggerSnapshot:
    level: int
    propagate: bool
    handlers: list[logging.Handler]
    filters: list[logging.Filter]
    disabled: bool


_BASE_LOGGER_NAMES = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "httpx",
    "httpcore",
    "wxverify",
)


def _normalized_logger_names() -> list[str]:
    names = set(_BASE_LOGGER_NAMES)
    for name, obj in list(logging.root.manager.loggerDict.items()):
        if isinstance(obj, logging.PlaceHolder):
            continue
        if name == "wxverify" or name.startswith("wxverify."):
            names.add(name)
    return sorted(names)


def _snapshot_logger(name: str) -> _LoggerSnapshot:
    lg = logging.getLogger(name)
    return _LoggerSnapshot(
        level=lg.level,
        propagate=lg.propagate,
        handlers=list(lg.handlers),
        filters=list(lg.filters),
        disabled=lg.disabled,
    )


def _restore_logger(name: str, saved: _LoggerSnapshot) -> None:
    lg = logging.getLogger(name)
    lg.setLevel(saved.level)
    lg.propagate = saved.propagate
    lg.handlers = list(saved.handlers)
    lg.filters = list(saved.filters)
    lg.disabled = saved.disabled


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


@dataclass
class _Server:
    port: int
    records: list[logging.LogRecord]
    formatted: list[str]
    requests: list[tuple[str, str, dict[str, str]]]
    db_path: Path
    stop_calls: list[object] = field(default_factory=list)
    thread_exc: list[BaseException] = field(default_factory=list)


@contextmanager
def _run_server(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    log_level: str = "INFO",
    wc_handler: Callable[[httpx.Request], httpx.Response] | None = None,
    om_handler: Callable[[httpx.Request], httpx.Response] | None = None,
    backoff_stub: Callable[[sqlite3.Connection, httpx.Response], str | None]
    | bool
    | None = None,
    extra_routes: Sequence[tuple[str, Callable[[], Awaitable[None]]]] = (),
) -> Iterator[_Server]:
    close_db()
    db_path = tmp_path / "wxverify.db"
    options_path = tmp_path / "missing-options.json"
    monkeypatch.setattr(config, "db_path", str(db_path))
    monkeypatch.setattr(config, "options_path", str(options_path))
    monkeypatch.setattr(config, "standalone_origin", None)
    monkeypatch.setattr(config, "ingress_root_path", config.ingress_root_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", KEY)
    monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)

    requests: list[tuple[str, str, dict[str, str]]] = []
    router = _make_router(wc_handler, om_handler, requests)
    real_async_client = httpx.AsyncClient

    def factory(*args: object, **kwargs: object) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(router), **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(httpx, "AsyncClient", factory)

    if backoff_stub is True:
        monkeypatch.setattr(
            "wxverify.api.routes.stations.record_http_backoff",
            lambda conn, response: None,
        )
    elif backoff_stub not in (None, False):
        monkeypatch.setattr(
            "wxverify.api.routes.stations.record_http_backoff", backoff_stub
        )

    names = _normalized_logger_names()
    saved_loggers = {name: _snapshot_logger(name) for name in names}
    saved_disable_level = logging.root.manager.disable
    logging.disable(logging.NOTSET)

    for name in names:
        lg = logging.getLogger(name)
        lg.setLevel(logging.NOTSET)
        lg.propagate = True
        lg.handlers = []
        lg.disabled = False
        lg.filters = []

    wire_level = logging.DEBUG if log_level == "DEBUG" else logging.WARNING
    logging.getLogger("httpx").setLevel(wire_level)
    logging.getLogger("httpcore").setLevel(wire_level)
    redact_wc = RedactUrlSecretsFilter()
    redact_hc = RedactUrlSecretsFilter()
    logging.getLogger("httpx").addFilter(redact_wc)
    logging.getLogger("httpcore").addFilter(redact_hc)

    root = logging.getLogger()
    saved_root_level = root.level
    saved_root_handlers = list(root.handlers)
    formatter = logging.Formatter("%(name)s %(levelname)s %(message)s")
    handler = _ListHandler(formatter)
    root.handlers = [handler]
    root.setLevel(logging.DEBUG if log_level == "DEBUG" else logging.INFO)

    stop_calls: list[object] = []
    app: FastAPI = create_app(root_path="", _stop_process=stop_calls.append)
    for path, endpoint in extra_routes:
        app.add_api_route(path, endpoint, methods=["GET"])

    server_config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=0,
        log_config=None,
        log_level=None,
        access_log=True,
        lifespan="on",
        http="h11",
        ws="none",
        timeout_graceful_shutdown=5,
    )
    server = uvicorn.Server(server_config)
    thread_exc: list[BaseException] = []

    def _run() -> None:
        try:
            asyncio.run(server.serve())
        except BaseException as exc:  # noqa: BLE001 -- recorded, never swallowed
            thread_exc.append(exc)

    thread = threading.Thread(target=_run, daemon=True, name="wxverify-test-uvicorn")
    thread.start()
    try:
        deadline = time.monotonic() + 15.0
        while not server.started:
            if thread_exc:
                raise AssertionError(
                    f"server thread failed to start: {thread_exc[0]!r}"
                )
            if time.monotonic() > deadline:
                raise AssertionError("server did not start within 15s")
            time.sleep(0.01)
        port = server.servers[0].sockets[0].getsockname()[1]
        yield _Server(
            port=port,
            records=handler.records,
            formatted=handler.formatted,
            requests=requests,
            db_path=db_path,
            stop_calls=stop_calls,
            thread_exc=thread_exc,
        )
    finally:
        server.should_exit = True
        thread.join(10)
        if thread.is_alive():
            server.force_exit = True
            thread.join(5)
        alive = thread.is_alive()
        close_db()
        root.handlers = saved_root_handlers
        root.setLevel(saved_root_level)
        logging.getLogger("httpx").removeFilter(redact_wc)
        logging.getLogger("httpcore").removeFilter(redact_hc)
        for name in names:
            _restore_logger(name, saved_loggers[name])
        logging.disable(saved_disable_level)
        assert not alive, "uvicorn server thread did not stop"


# ---------------------------------------------------------------------------
# Client-side helpers
# ---------------------------------------------------------------------------


def _client_station_flow(
    port: int,
    *,
    station_body: dict[str, object],
    target_override: int | None = None,
) -> tuple[httpx.Response, int]:
    base = f"http://127.0.0.1:{port}"
    with httpx.Client(
        base_url=base, timeout=10.0, headers={"Connection": "close", "Origin": base}
    ) as client:
        csrf_token = client.get("/api/csrf").json()["csrf_token"]
        site_resp = client.post(
            "/api/sites",
            json={
                "name": "Test site",
                "forecast_lat": 0.0,
                "forecast_lon": 0.0,
                "elevation_m": 0.0,
                "timezone": "UTC",
            },
            headers={"X-CSRF-Token": csrf_token},
        )
        assert site_resp.status_code == 200, site_resp.text
        site_id = site_resp.json()["id"]
        target = target_override if target_override is not None else site_id
        station_resp = client.post(
            f"/api/sites/{target}/stations",
            json=station_body,
            headers={"X-CSRF-Token": csrf_token},
        )
    return station_resp, site_id


def _needles_in_text(text: str) -> list[str]:
    return [n for n in NEEDLES if n in text]


def _response_leak(response: httpx.Response) -> list[str]:
    hits = [f"body needle {n!r}" for n in _needles_in_text(response.text)]
    for key, value in response.headers.items():
        hits += [f"header {key} needle {n!r}" for n in _needles_in_text(value)]
    return hits


def leak_report(
    records: list[logging.LogRecord],
    formatted: list[str],
    *,
    allow_debug: bool,
    skip: Callable[[logging.LogRecord], bool] = lambda _r: False,
) -> list[str]:
    violations: list[str] = []
    for record, text in zip(records, formatted, strict=True):
        if skip(record):
            continue
        allowed = allow_debug and any(pat.fullmatch(text) for pat in DEBUG_ALLOWLIST)
        needles = ("ci-placeholder",) if allowed else NEEDLES
        for needle in needles:
            if needle in text:
                violations.append(f"needle {needle!r} in record: {text}")
        if record.levelno >= logging.ERROR:
            violations.append(f"ERROR+ record: {text}")
        if record.exc_info is not None:
            violations.append(f"exc_info record: {text}")
    return violations


_STARTED_RE = re.compile(r"Started server process \[\d+\]")
_FINISHED_RE = re.compile(r"Finished server process \[\d+\]")


def _assert_server_controls(records: list[logging.LogRecord]) -> None:
    assert any(
        r.name == "uvicorn.error"
        and r.levelno == logging.INFO
        and _STARTED_RE.fullmatch(r.getMessage())
        for r in records
    )
    assert any(
        r.name == "uvicorn.error"
        and r.levelno == logging.INFO
        and _FINISHED_RE.fullmatch(r.getMessage())
        for r in records
    )


def _assert_access_control(
    records: list[logging.LogRecord], method: str, path_pattern: str, status: int
) -> None:
    pattern = re.compile(rf'"{method} {path_pattern} HTTP/1\.1" {status}\b')
    assert any(
        r.name == "uvicorn.access" and pattern.search(r.getMessage()) for r in records
    )


def _read_counts(db_path: Path) -> tuple[int, int]:
    conn = sqlite3.connect(str(db_path), timeout=5)
    try:
        stations_count = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
        backoff_count = conn.execute("SELECT COUNT(*) FROM domain_backoffs").fetchone()[
            0
        ]
    finally:
        conn.close()
    return int(stations_count), int(backoff_count)


_URL_RE = re.compile(r'https?://[^\s"]+')


def _provider_http_response_count(records: list[logging.LogRecord], host: str) -> int:
    """Count records whose message holds a URL for ``host`` (plan §5.3)."""
    count = 0
    for r in records:
        message = r.getMessage()
        if "HTTP Request:" not in message:
            continue
        for match in _URL_RE.findall(message):
            if urlsplit(match).hostname == host:
                count += 1
                break
    return count


def _warning_records(records: list[logging.LogRecord]) -> list[logging.LogRecord]:
    return [
        r
        for r in records
        if r.name == "wxverify.api.routes.stations" and r.levelno == logging.WARNING
    ]


# ---------------------------------------------------------------------------
# F cases: every provider failure answers safely and logs nothing secret
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("variant", ["INFO", "DEBUG"])
@pytest.mark.parametrize("case", FCASES, ids=[c.case_id for c in FCASES])
def test_provider_failure_answers_safely_and_logs_nothing_secret(
    case: FCase, variant: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _run_server(
        monkeypatch,
        tmp_path,
        log_level=variant,
        wc_handler=case.wc,
        om_handler=case.om,
        backoff_stub=case.backoff_stub,
    ) as srv:
        response, _site_id = _client_station_flow(
            srv.port, station_body={"pws_station_id": STATION_ID}
        )

    assert srv.stop_calls == []
    assert srv.thread_exc == []
    assert response.status_code == case.http_status
    assert response.json() == {"error": case.text}
    assert _response_leak(response) == []

    wc_reqs = [r for r in srv.requests if r[0] == "api.weather.com"]
    om_reqs = [r for r in srv.requests if r[0] == "api.open-meteo.com"]
    assert len(wc_reqs) == case.w_count
    assert len(om_reqs) == case.o_count
    if wc_reqs:
        assert wc_reqs[0][2].get("stationId") == STATION_ID
        assert wc_reqs[0][2].get("apiKey") == KEY
    if om_reqs:
        assert om_reqs[0][2].get("latitude") == str(LAT)
        assert om_reqs[0][2].get("longitude") == str(LON)

    warnings = _warning_records(srv.records)
    assert len(warnings) == 1
    assert re.fullmatch(case.warning_pattern, warnings[0].getMessage())

    violations = leak_report(
        srv.records, srv.formatted, allow_debug=(variant == "DEBUG")
    )
    assert violations == []

    if variant == "DEBUG":
        adapter_debug_lines = [
            t for t in srv.formatted if DEBUG_ALLOWLIST[0].fullmatch(t)
        ]
        assert len(adapter_debug_lines) == 1

        expected_wc_http = 0 if case.case_id in _WC_RAISES_IDS else case.w_count
        expected_om_http = 0 if case.case_id in _OM_RAISES_IDS else case.o_count
        assert (
            _provider_http_response_count(srv.records, "api.weather.com")
            == expected_wc_http
        )
        assert (
            _provider_http_response_count(srv.records, "api.open-meteo.com")
            == expected_om_http
        )
        assert _provider_http_response_count(srv.records, "127.0.0.1") == 3

    stations_count, backoff_count = _read_counts(srv.db_path)
    assert stations_count == 0
    assert backoff_count == 0

    _assert_server_controls(srv.records)
    _assert_access_control(
        srv.records, "POST", r"/api/sites/\d+/stations", case.http_status
    )


# ---------------------------------------------------------------------------
# C1: a real 429/503 still backs off (the parallel domain-backoff path stays
# intact while the leak fix is in place)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("which", ["wc429", "wc503", "om429", "om503"])
def test_throttle_still_backs_off(
    which: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    if which == "wc429":
        wc_handler, om_handler, domain, status, w, o = (
            _status(429),
            None,
            "api.weather.com",
            429,
            1,
            0,
        )
    elif which == "wc503":
        wc_handler, om_handler, domain, status, w, o = (
            _status(503),
            None,
            "api.weather.com",
            503,
            1,
            0,
        )
    elif which == "om429":
        wc_handler, om_handler, domain, status, w, o = (
            None,
            _status(429),
            "api.open-meteo.com",
            429,
            1,
            1,
        )
    else:
        wc_handler, om_handler, domain, status, w, o = (
            None,
            _status(503),
            "api.open-meteo.com",
            503,
            1,
            1,
        )

    with _run_server(
        monkeypatch, tmp_path, wc_handler=wc_handler, om_handler=om_handler
    ) as srv:
        response, _site_id = _client_station_flow(
            srv.port, station_body={"pws_station_id": STATION_ID}
        )

    assert srv.stop_calls == []
    assert srv.thread_exc == []
    _assert_server_controls(srv.records)
    _assert_access_control(srv.records, "POST", r"/api/sites/\d+/stations", 503)
    assert leak_report(srv.records, srv.formatted, allow_debug=False) == []

    assert response.status_code == 503
    body = response.json()
    assert body["error"] == "budget exhausted"
    next_attempt_at = body["next_attempt_at"]
    assert _response_leak(response) == []

    conn = sqlite3.connect(str(srv.db_path))
    try:
        row = conn.execute(
            "SELECT next_attempt_at, retry_count FROM domain_backoffs WHERE domain=?",
            (domain,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == next_attempt_at
    assert row[1] == 1

    domain_backoff_warnings = [
        r
        for r in srv.records
        if r.name == "wxverify.worker.domain_backoff" and r.levelno == logging.WARNING
    ]
    assert len(domain_backoff_warnings) == 1
    assert re.fullmatch(
        rf"domain backoff activated domain={re.escape(domain)} status={status} "
        r"retry=1 until=.+",
        domain_backoff_warnings[0].getMessage(),
    )
    assert _warning_records(srv.records) == []

    stations_count, _backoff_count = _read_counts(srv.db_path)
    assert stations_count == 0
    wc_reqs = [r for r in srv.requests if r[0] == "api.weather.com"]
    om_reqs = [r for r in srv.requests if r[0] == "api.open-meteo.com"]
    assert len(wc_reqs) == w
    assert len(om_reqs) == o


# ---------------------------------------------------------------------------
# C2: a stored backoff still defers before the provider is ever called
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("domain", ["api.weather.com", "api.open-meteo.com"])
def test_stored_backoff_still_defers(
    domain: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _run_server(monkeypatch, tmp_path) as srv:
        conn = sqlite3.connect(str(srv.db_path), timeout=5)
        try:
            conn.execute(
                "INSERT INTO domain_backoffs "
                "(domain, next_attempt_at, retry_count) VALUES (?, ?, ?)",
                (domain, "2099-01-01T00:00:00Z", 1),
            )
            conn.commit()
        finally:
            conn.close()
        response, _site_id = _client_station_flow(
            srv.port, station_body={"pws_station_id": STATION_ID}
        )

    assert srv.stop_calls == []
    assert srv.thread_exc == []
    _assert_server_controls(srv.records)
    _assert_access_control(srv.records, "POST", r"/api/sites/\d+/stations", 503)
    assert leak_report(srv.records, srv.formatted, allow_debug=False) == []

    assert response.status_code == 503
    assert response.json() == {
        "error": "budget exhausted",
        "next_attempt_at": "2099-01-01T00:00:00Z",
    }
    assert _response_leak(response) == []

    wc_reqs = [r for r in srv.requests if r[0] == "api.weather.com"]
    om_reqs = [r for r in srv.requests if r[0] == "api.open-meteo.com"]
    if domain == "api.weather.com":
        assert len(wc_reqs) == 0
        assert len(om_reqs) == 0
    else:
        assert len(wc_reqs) == 1
        assert len(om_reqs) == 0

    assert _warning_records(srv.records) == []
    stations_count, _backoff_count = _read_counts(srv.db_path)
    assert stations_count == 0


# ---------------------------------------------------------------------------
# C3: a missing site still 404s before any provider call
# ---------------------------------------------------------------------------


def test_missing_site_still_404s(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _run_server(monkeypatch, tmp_path) as srv:
        response, _site_id = _client_station_flow(
            srv.port, station_body={"pws_station_id": STATION_ID}, target_override=999
        )

    assert srv.stop_calls == []
    assert srv.thread_exc == []
    _assert_server_controls(srv.records)
    _assert_access_control(srv.records, "POST", r"/api/sites/\d+/stations", 404)
    assert leak_report(srv.records, srv.formatted, allow_debug=False) == []

    assert response.status_code == 404
    assert response.json() == {"error": "site not found"}
    assert _response_leak(response) == []
    assert srv.requests == []
    assert _warning_records(srv.records) == []


# ---------------------------------------------------------------------------
# C4: success still creates the station (positive control)
# ---------------------------------------------------------------------------


def test_success_still_creates_the_station(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _run_server(monkeypatch, tmp_path) as srv:
        response, _site_id = _client_station_flow(
            srv.port, station_body={"pws_station_id": STATION_ID}
        )

    assert response.status_code == 200
    body = response.json()
    assert body["pws_station_id"] == STATION_ID
    assert body["lat"] == LAT
    assert body["lon"] == LON
    assert body["dem_elevation_m"] == 1234.5

    conn = sqlite3.connect(str(srv.db_path))
    try:
        row = conn.execute(
            "SELECT pws_station_id, lat, lon, dem_elevation_m FROM stations"
        ).fetchone()
        backoff_count = conn.execute("SELECT COUNT(*) FROM domain_backoffs").fetchone()[
            0
        ]
    finally:
        conn.close()
    assert row == (STATION_ID, LAT, LON, 1234.5)
    assert backoff_count == 0

    wc_reqs = [r for r in srv.requests if r[0] == "api.weather.com"]
    om_reqs = [r for r in srv.requests if r[0] == "api.open-meteo.com"]
    assert len(wc_reqs) == 1
    assert len(om_reqs) == 1

    assert _warning_records(srv.records) == []
    violations = leak_report(srv.records, srv.formatted, allow_debug=False)
    assert violations == []
    _assert_server_controls(srv.records)
    _assert_access_control(srv.records, "POST", r"/api/sites/\d+/stations", 200)


# ---------------------------------------------------------------------------
# C5: a backoff write failure is a plain 500, never carrying the URL
# ---------------------------------------------------------------------------


def test_backoff_write_failure_is_a_500_without_the_url(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[int] = []
    with _run_server(
        monkeypatch,
        tmp_path,
        wc_handler=_status(401),
        backoff_stub=_make_db_fail_stub(calls),
    ) as srv:
        response, _site_id = _client_station_flow(
            srv.port, station_body={"pws_station_id": STATION_ID}
        )

    assert srv.stop_calls == []
    assert srv.thread_exc == []
    _assert_server_controls(srv.records)
    _assert_access_control(srv.records, "POST", r"/api/sites/\d+/stations", 500)

    assert response.status_code == 500
    assert calls == [1]
    assert _response_leak(response) == []
    stations_count, _backoff_count = _read_counts(srv.db_path)
    assert stations_count == 0

    exc_pairs = [
        (r, t)
        for r, t in zip(srv.records, srv.formatted, strict=True)
        if r.name == "uvicorn.error"
        and r.levelno == logging.ERROR
        and r.exc_info is not None
    ]
    assert len(exc_pairs) == 1
    record, text = exc_pairs[0]
    assert record.getMessage().strip() == "Exception in ASGI application"
    assert "synthetic backoff write failure" in text
    assert _needles_in_text(text) == []

    skip_id = id(record)
    violations = leak_report(
        srv.records, srv.formatted, allow_debug=False, skip=lambda r: id(r) == skip_id
    )
    assert violations == []


# ---------------------------------------------------------------------------
# H1/H2: the harness's own needle scan does not false-negative on an
# unrelated unhandled exception's traceback text
# ---------------------------------------------------------------------------


async def _raise_runtime_error() -> None:
    raise RuntimeError(SYNTHETIC_TEXT)


async def _raise_chained_runtime_error() -> None:
    try:
        raise ValueError(SYNTHETIC_TEXT)
    except ValueError as exc:
        raise RuntimeError("outer") from exc


def test_h1_unrelated_traceback_is_flagged_not_silently_passed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with (
        _run_server(
            monkeypatch, tmp_path, extra_routes=[("/__h1", _raise_runtime_error)]
        ) as srv,
        httpx.Client(
            base_url=f"http://127.0.0.1:{srv.port}",
            timeout=10.0,
            headers={"Connection": "close"},
        ) as client,
    ):
        response = client.get("/__h1")

    assert srv.stop_calls == []
    assert srv.thread_exc == []
    _assert_server_controls(srv.records)

    assert response.status_code == 500
    exc_pairs = [
        (r, t)
        for r, t in zip(srv.records, srv.formatted, strict=True)
        if r.name == "uvicorn.error"
        and r.levelno == logging.ERROR
        and r.exc_info is not None
    ]
    assert len(exc_pairs) == 1
    record, text = exc_pairs[0]
    assert "ci-placeholder" in text
    assert _needles_in_text(record.getMessage()) == []
    skip_id = id(record)
    violations = leak_report(
        srv.records, srv.formatted, allow_debug=False, skip=lambda r: id(r) == skip_id
    )
    assert violations == []


def test_h2_unrelated_chained_traceback_is_flagged_not_silently_passed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with (
        _run_server(
            monkeypatch,
            tmp_path,
            extra_routes=[("/__h2", _raise_chained_runtime_error)],
        ) as srv,
        httpx.Client(
            base_url=f"http://127.0.0.1:{srv.port}",
            timeout=10.0,
            headers={"Connection": "close"},
        ) as client,
    ):
        response = client.get("/__h2")

    assert srv.stop_calls == []
    assert srv.thread_exc == []
    _assert_server_controls(srv.records)

    assert response.status_code == 500
    exc_pairs = [
        (r, t)
        for r, t in zip(srv.records, srv.formatted, strict=True)
        if r.name == "uvicorn.error"
        and r.levelno == logging.ERROR
        and r.exc_info is not None
    ]
    assert len(exc_pairs) == 1
    record, text = exc_pairs[0]
    assert "ci-placeholder" in text
    skip_id = id(record)
    violations = leak_report(
        srv.records, srv.formatted, allow_debug=False, skip=lambda r: id(r) == skip_id
    )
    assert violations == []


# ---------------------------------------------------------------------------
# X1/X2: direct calls into create_station, no server
# ---------------------------------------------------------------------------


def _seed_site(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone) "
        "VALUES (?, ?, ?, ?, ?)",
        ("Test site", 0.0, 0.0, 0.0, "UTC"),
    )
    site_id = cur.lastrowid
    assert site_id is not None
    return int(site_id)


@pytest.mark.parametrize("variant", ["401", "429", "dbfail"])
def test_no_provider_error_in_the_raised_chain(
    variant: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "wxverify.db"
    monkeypatch.setattr(config, "db_path", str(db_path))
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", KEY)

    wc_handler = _status(401) if variant in ("401", "dbfail") else _status(429)
    requests: list[tuple[str, str, dict[str, str]]] = []
    router = _make_router(wc_handler, None, requests)
    real_async_client = httpx.AsyncClient

    def factory(*args: object, **kwargs: object) -> httpx.AsyncClient:
        return real_async_client(*args, transport=httpx.MockTransport(router), **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(httpx, "AsyncClient", factory)

    calls: list[int] = []
    if variant == "dbfail":
        monkeypatch.setattr(
            "wxverify.api.routes.stations.record_http_backoff",
            _make_db_fail_stub(calls),
        )

    caught: BaseException | None = None

    async def run() -> None:
        nonlocal caught
        init_db(str(db_path))
        try:
            site_id = await get_db().write(_seed_site)
            scope = {"type": "http", "method": "POST", "path": "/", "headers": []}
            try:
                await stations.create_station(
                    Request(scope), site_id, StationCreate(pws_station_id=STATION_ID)
                )
            except Exception as exc:
                caught = exc
        finally:
            close_db()

    asyncio.run(run())

    assert caught is not None
    if variant == "401":
        assert isinstance(caught, ApiError)
        assert caught.status_code == 502
    elif variant == "429":
        assert isinstance(caught, JobDeferred)
    else:
        assert isinstance(caught, sqlite3.OperationalError)
        assert calls == [1]

    chain: list[BaseException] = []
    seen = {id(caught)}
    queue: list[BaseException] = [caught]
    while queue:
        current = queue.pop(0)
        chain.append(current)
        for nxt in (current.__cause__, current.__context__):
            if nxt is not None and id(nxt) not in seen:
                seen.add(id(nxt))
                queue.append(nxt)
    assert not any(isinstance(exc, httpx.HTTPError) for exc in chain)

    import traceback

    rendered = "".join(traceback.format_exception(caught))
    assert _needles_in_text(rendered) == []


def test_cancel_passes_through_and_releases_the_lock(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "wxverify.db"
    monkeypatch.setattr(config, "db_path", str(db_path))
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", KEY)

    async def _cancelling_validate_station(station_id: str, api_key: str) -> PwsStation:
        raise asyncio.CancelledError()

    monkeypatch.setattr(
        "wxverify.api.routes.stations.validate_station", _cancelling_validate_station
    )

    cancelled_caught = False
    lock_locked_after: bool | None = None

    async def run() -> None:
        nonlocal cancelled_caught, lock_locked_after
        init_db(str(db_path))
        try:
            site_id = await get_db().write(_seed_site)
            scope = {"type": "http", "method": "POST", "path": "/", "headers": []}
            try:
                await stations.create_station(
                    Request(scope), site_id, StationCreate(pws_station_id=STATION_ID)
                )
            except asyncio.CancelledError:
                cancelled_caught = True
            lock_locked_after = weathercom_call_lock().locked()
        finally:
            close_db()

    with caplog.at_level(logging.WARNING, logger="wxverify.api.routes.stations"):
        asyncio.run(run())
        logging.getLogger("wxverify.api.routes.stations").warning("x2 capture sentinel")
    assert cancelled_caught
    assert lock_locked_after is False
    assert (
        len(
            [
                r.getMessage()
                for r in caplog.records
                if r.getMessage() == "x2 capture sentinel"
            ]
        )
        == 1
    )
    assert not [
        r for r in caplog.records if r.getMessage().startswith("station add failed")
    ]
