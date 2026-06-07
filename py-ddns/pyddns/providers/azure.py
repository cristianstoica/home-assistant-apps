# pyright: strict
"""The `azure` provider — the API archetype.

The client does the work: authenticate (SP client-credentials), GET the current
A record-set, and create-or-replace it via the GA ``2018-05-01`` management
surface. Pins **GA** ``2018-05-01`` (not the ``2023-07-01-preview`` the Bicep
carries) — a long-lived unattended client must not depend on a preview version
Azure can retire.

Status handling (per the plan):

* Token endpoint auth failure (``invalid_client`` / ``AADSTS7000222`` / 400/401)
  → **terminal** (a bad secret never self-heals): raise `TerminalError`.
* Management ``401`` with a *cached* token → invalidate, re-acquire **once**,
  retry once; a ``401`` after a fresh token → **terminal**.
* ``403`` → **terminal** (role not assigned / wrong scope).
* Record ``GET 404`` → **missing record, not a failure** → `read_current` returns
  ``None`` (drives the first-run create path). Never logged as an error.
* ``429`` / ``5xx`` / network / timeout → **transient** (`TransientError`) for the
  updater's bounded backoff.

Secret-safe: `clientSecret` and the bearer token are never logged; the token
endpoint's error body is parsed for the AAD error code but never echoed wholesale.
"""

from __future__ import annotations

import json
import logging
from ipaddress import IPv4Address
from typing import cast
from urllib.parse import urlencode

from ..errors import TerminalError, TransientError
from ..httpclient import HttpClient, HttpError
from ..models import ApplyAction, ApplyResult, AzureToken, Clock
from ..redact import sanitize

_log = logging.getLogger("pyddns")

_API_VERSION = "2018-05-01"
_LOGIN_HOST = "https://login.microsoftonline.com"
_MGMT_HOST = "https://management.azure.com"
_SCOPE = "https://management.azure.com/.default"

# Per the plan: token POST 10s, record GET/PUT 10s.
_TOKEN_TIMEOUT_S = 10.0
_RECORD_TIMEOUT_S = 10.0
# Refresh the cached token ~5 min before expiry.
_TOKEN_REFRESH_SKEW_S = 300.0


def _token_url(tenant_id: str) -> str:
    return f"{_LOGIN_HOST}/{tenant_id}/oauth2/v2.0/token"


def record_url(token: AzureToken, label: str) -> str:
    """The management URL for the A record-set (GET + create-or-replace PUT)."""
    path = (
        f"/subscriptions/{token.subscription_id}"
        f"/resourceGroups/{token.resource_group}"
        f"/providers/Microsoft.Network/dnszones/{token.zone}"
        f"/A/{label}"
    )
    return f"{_MGMT_HOST}{path}?api-version={_API_VERSION}"


def record_body(ttl: int, ip: IPv4Address) -> dict[str, object]:
    """The create-or-replace A record-set request body (a full replace)."""
    return {"properties": {"TTL": ttl, "ARecords": [{"ipv4Address": str(ip)}]}}


class _CachedToken:
    """A bearer token and the monotonic time after which it must be refreshed."""

    def __init__(self, value: str, refresh_after: float) -> None:
        self.value = value
        self.refresh_after = refresh_after


class AzureProvider:
    """The `azure` (API archetype) `DnsProvider`."""

    def __init__(
        self,
        token: AzureToken,
        record_label: str,
        ttl: int,
        http: HttpClient,
        clock: Clock,
    ) -> None:
        self._token = token
        self._label = record_label
        self._ttl = ttl
        self._http = http
        self._clock = clock
        self._cached: _CachedToken | None = None

    # --- token --------------------------------------------------------------

    def _acquire_token(self) -> str:
        """Acquire + cache a client-credentials bearer token.

        Any auth failure is terminal (`TerminalError`); a transport/5xx failure
        is transient (`TransientError`). The `clientSecret` is in the POST body
        only and never logged; a token-endpoint error body is parsed for the AAD
        error code but never echoed wholesale.
        """
        body = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self._token.client_id,
                "client_secret": self._token.client_secret,
                "scope": _SCOPE,
            }
        ).encode("ascii")
        url = _token_url(self._token.tenant_id)
        try:
            resp = self._http.request(
                "POST",
                url,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=body,
                timeout=_TOKEN_TIMEOUT_S,
            )
        except HttpError as exc:
            if exc.status is None or exc.status >= 500 or exc.status == 429:
                raise TransientError(
                    f"token endpoint transient failure: {exc}",
                    retry_after=exc.retry_after,
                ) from None
            raise TerminalError(self._auth_failure_message(exc)) from None
        try:
            parsed = resp.json()
        except json.JSONDecodeError:
            raise TransientError(
                "token endpoint returned an unparseable body"
            ) from None
        if not isinstance(parsed, dict):
            raise TransientError("token endpoint returned a non-object body")
        body_obj = cast(dict[str, object], parsed)
        access = body_obj.get("access_token")
        expires = body_obj.get("expires_in")
        if not isinstance(access, str) or not access:
            raise TerminalError("token endpoint returned no access_token")
        ttl_seconds = float(expires) if isinstance(expires, (int, float)) else 3600.0
        refresh_after = self._clock() + max(0.0, ttl_seconds - _TOKEN_REFRESH_SKEW_S)
        self._cached = _CachedToken(access, refresh_after)
        return access

    def _auth_failure_message(self, exc: HttpError) -> str:
        """A loud, secret-free auth-failure message; flags the expired-secret signal.

        The AAD error *code* (e.g. ``AADSTS7000222``) is surfaced for diagnosis;
        the secret is scrubbed from any echoed text.
        """
        code = ""
        try:
            parsed = json.loads(exc.body)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            body_obj = cast(dict[str, object], parsed)
            err = body_obj.get("error")
            if isinstance(err, str):
                code = err
            desc = body_obj.get("error_description")
            if isinstance(desc, str) and "AADSTS7000222" in desc:
                code = f"{code} (AADSTS7000222 expired client secret)".strip()
        safe = sanitize(code, (self._token.client_secret,))
        return f"azure token auth failed (terminal — fix the SP credential): {safe}"

    def _bearer(self, force_refresh: bool) -> str:
        """Return a valid bearer token, refreshing if forced or near expiry."""
        cached = self._cached
        if force_refresh or cached is None or self._clock() >= cached.refresh_after:
            return self._acquire_token()
        return cached.value

    # --- read / write -------------------------------------------------------

    def read_current(self) -> IPv4Address | None:
        """GET the current A record-set value. ``404`` → ``None`` (create path).

        A ``401`` on a cached token re-acquires once then retries; any other
        non-2xx is classified terminal/transient. Returns the first A record's
        IPv4, or ``None`` for a 404 / empty record-set.
        """
        resp = self._mgmt_request("GET", record_url(self._token, self._label), None)
        if resp is None:
            return None  # 404 -> missing record
        try:
            parsed = resp.json()
        except json.JSONDecodeError:
            raise TransientError("record GET returned an unparseable body") from None
        return _first_a_record(parsed)

    def apply(self, detected_ip: IPv4Address | None) -> ApplyResult:
        """Create-or-replace the A record-set to `detected_ip`.

        The API archetype **requires** a known IP: a ``None`` is a `SKIPPED_NO_IP`
        no-op (nothing valid to write). A 2xx PUT is the authoritative replace, so
        the result carries `written_ip` for the updater to persist.
        """
        if detected_ip is None:
            return ApplyResult(
                ApplyAction.SKIPPED_NO_IP,
                "no valid egress IP detected; holding last-good",
                None,
            )
        url = record_url(self._token, self._label)
        data = json.dumps(record_body(self._ttl, detected_ip)).encode("utf-8")
        self._mgmt_request("PUT", url, data)
        return ApplyResult(
            ApplyAction.WROTE_KNOWN_IP,
            f"PUT A/{self._label} ttl={self._ttl} -> {detected_ip}",
            detected_ip,
        )

    def _mgmt_request(self, method: str, url: str, data: bytes | None):  # type: ignore[no-untyped-def]
        """Run a management request with the cached-token 401-retry-once policy.

        Returns the `HttpResponse` on 2xx, ``None`` for a GET 404 (missing
        record), raises `TerminalError` (401-after-fresh / 403 / other 4xx) or
        `TransientError` (429 / 5xx / network).
        """
        used_cached = self._cached is not None
        token = self._bearer(force_refresh=False)
        try:
            return self._mgmt_call(method, url, data, token)
        except HttpError as exc:
            if exc.status == 404 and method == "GET":
                return None
            if exc.status == 401 and used_cached:
                # Cached token may be stale: invalidate, re-acquire once, retry.
                self._cached = None
                fresh = self._bearer(force_refresh=True)
                try:
                    return self._mgmt_call(method, url, data, fresh)
                except HttpError as retry_exc:
                    raise self._classify(retry_exc, method) from None
            raise self._classify(exc, method) from None

    def _mgmt_call(self, method: str, url: str, data: bytes | None, token: str):  # type: ignore[no-untyped-def]
        headers = {"Authorization": f"Bearer {token}"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        return self._http.request(
            method, url, headers=headers, data=data, timeout=_RECORD_TIMEOUT_S
        )

    def _classify(self, exc: HttpError, method: str) -> Exception:
        """Map a management `HttpError` to a terminal/transient domain error."""
        if exc.status == 404 and method == "GET":
            return TransientError("unexpected 404 classification")  # pragma: no cover
        if exc.status is None or exc.status >= 500 or exc.status == 429:
            return TransientError(
                f"management transient failure: {exc}", retry_after=exc.retry_after
            )
        if exc.status == 403:
            return TerminalError(
                "azure management 403 (terminal — role not assigned or wrong scope)"
            )
        if exc.status == 401:
            return TerminalError(
                "azure management 401 after a fresh token (terminal — authz problem)"
            )
        return TerminalError(f"azure management {exc.status} (terminal): {exc}")

    # --- dry-run ------------------------------------------------------------

    def plan(self, detected_ip: IPv4Address | None) -> str:
        """A redacted, secret-free description of the planned PUT (for --dry-run).

        Reports method + host + record label + the redacted body (TTL + IP) only
        — never the token-request material, bearer token, or `clientSecret`.
        """
        target = detected_ip if detected_ip is not None else "<no egress IP detected>"
        return (
            f"azure: would PUT A record-set {self._label} in zone {self._token.zone} "
            f"on management.azure.com (api-version={_API_VERSION}); "
            f"body: ttl={self._ttl} ARecords=[{target}]"
        )


def _first_a_record(parsed: object) -> IPv4Address | None:
    """Extract the first A record's IPv4 from a record-set GET body, else ``None``."""
    if not isinstance(parsed, dict):
        return None
    body_obj = cast(dict[str, object], parsed)
    props = body_obj.get("properties")
    if not isinstance(props, dict):
        return None
    props_obj = cast(dict[str, object], props)
    records = props_obj.get("ARecords")
    if not isinstance(records, list):
        return None
    records_list = cast(list[object], records)
    for record in records_list:
        if isinstance(record, dict):
            record_obj = cast(dict[str, object], record)
            ipv4 = record_obj.get("ipv4Address")
            if isinstance(ipv4, str):
                try:
                    return IPv4Address(ipv4)
                except (ValueError, OSError):
                    continue
    return None
