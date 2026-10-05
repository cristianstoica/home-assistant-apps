# pyright: strict
r"""Check the container registry before an add-on release pushes anything.

The release workflow runs this script before every push, so a version that is
already published is never pushed again. Standard library only; Python 3.12+.

Usage (synthetic values)::

    python3 .github/scripts/publish_guard.py check \
      --registry-prefix ghcr.io/example --image-name addon --version 1.2.3 \
      --archs '["aarch64","amd64"]' --own-files-changed true --publish-mode true

    python3 .github/scripts/publish_guard.py tag \
      --ref ghcr.io/example/amd64-addon:1.2.3 --push true

``check`` looks up every architecture's tag in ``--archs`` order, then the
multi-architecture manifest tag (for the example above:
ghcr.io/example/aarch64-addon:1.2.3, ghcr.io/example/amd64-addon:1.2.3,
ghcr.io/example/addon:1.2.3). It looks up every tag first, then decides; the
first matching rule wins:

* any lookup failed: ``fail`` (exit 1), nothing is pushed;
* every tag missing: ``publish`` (exit 0);
* some tags present, some missing: ``fail`` (exit 1), the version is
  half-published;
* every tag present and the add-on's own files changed: ``fail`` (exit 1),
  bump the version;
* every tag present otherwise: ``skip`` (exit 0).

It writes ``decision``, ``reason`` and ``push`` to ``$GITHUB_OUTPUT`` and, when
``$GITHUB_STEP_SUMMARY`` is set, appends a table there. ``push`` is ``true`` only
for ``--publish-mode true`` and a ``publish`` decision, so the script can turn
publishing off but never on.

``tag`` re-checks the one ref a job is about to push. With ``--push true`` a
present tag or a failed lookup exits 1; with ``--push false`` only a failed
lookup does.

Credentials come from ``GHCR_LOOKUP_USER`` and ``GHCR_LOOKUP_TOKEN``. They are
traded for a registry token that can only pull, and neither is ever printed.
``--anonymous`` sends no credentials; it is for local runs, and workflows never
pass it.

A tag counts as missing only when the registry answers 404 with exactly one
error whose code is ``MANIFEST_UNKNOWN`` and whose message is
``manifest unknown``. Every other answer is a failed lookup, so the guard fails
closed.

When a lookup fails
-------------------

1. A timeout, connection error, 429 or 5xx: use "Re-run failed jobs".
2. A 404 whose code or message is not the allowlisted pair: ghcr may have
   reworded its message. Before adding the new body to the ABSENT allowlist,
   confirm independently that the ref really is missing and that a published
   tag still answers 200 with the four-type Accept header (``ACCEPT`` below).
   Then land the change as a fix-forward pull request with a test.
3. A 401 or 403 for an add-on that has never been published: not expected,
   because the workflow token gets ``404 manifest unknown`` for a missing name.
   Treat it as a one-off for the owner.
4. A 401 or 403 on every add-on, or ``workflow token not set``: the lookup
   credential or the workflow wiring is wrong. Fix the workflow.
5. A pull request from a fork: it is expected to fail closed.

Standing checks for any change to the guard
-------------------------------------------

An edit to this script or to either release workflow selects every add-on, and
once merged it runs with publishing on. Every pull request that changes them,
including a change to the ABSENT allowlist, must pass these checks:

a. It changes only ``.github/`` and is never combined with a version bump.
b. Its pull-request run shows exactly one ``skip`` decision per add-on, no
   ``publish``, no ``fail`` and no cancelled job.
c. No commit on the branch carries a CI skip instruction.
d. Registry digests for every add-on's tags are recorded immediately before
   the merge, with a check that shares no code with this script.
e. After the first run on main, the digests are compared with those records.
   Any change goes to the owner.
"""

import argparse
import base64
import dataclasses
import enum
import http.client
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from email.message import Message
from typing import IO, Protocol, cast

REGISTRY = "ghcr.io"
TIMEOUT_SECONDS = 20.0
MAX_BODY_BYTES = 64 * 1024
MAX_TEXT_CHARS = 200
ARCHITECTURES = frozenset({"aarch64", "amd64"})
ACCEPT = ",".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)
TOKEN_NOT_SET = "workflow token not set"
LOOKUP_FAILED_TAIL = (
    "Re-run only after a timeout, connection error, 429 or 5xx; other answers "
    'usually repeat, see "When a lookup fails" in publish_guard.py'
)

_COMPONENT = r"[a-z0-9]+(?:(?:\.|_|__|-+)[a-z0-9]+)*"
# OCI repository name grammar, at least owner/name.
REPOSITORY_RE = re.compile(rf"^{_COMPONENT}(?:/{_COMPONENT})+$")
# One or more name components: the owner part of --registry-prefix, --image-name.
_NAME_RE = re.compile(rf"^{_COMPONENT}(?:/{_COMPONENT})*$")
# OCI tag grammar.
TAG_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$")
_SINGLE_LINE_RE = re.compile(r"^[^\r\n]*$")
# A registry token goes into a request header, so it must be visible ASCII.
_HEADER_TOKEN_RE = re.compile(r"^[!-~]+$")
# Registry text never repeats the script's own outcome wording, so a failed
# lookup can never read like an "already published" or "half-published" result.
_ALREADY_PUBLISHED_RE = re.compile(r"(already) +(published)", re.IGNORECASE)
_HALF_PUBLISHED_RE = re.compile(r"(half)-(published)", re.IGNORECASE)
# Registry text keeps only these characters. Everything else is dropped,
# including line breaks, "%", and the "#", "[" and "]" of the runner's legacy
# "##[command]" form, which the runner recognises anywhere in a line.
_UNSAFE_TEXT_RE = re.compile(r"[^A-Za-z0-9 ._,:;()/=+'-]")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

# Network and HTTP-library failures. Anything else is a programming error and
# is not caught: the script exits 1 with a traceback and pushes nothing.
_NETWORK_ERRORS = (OSError, http.client.HTTPException)


class Outcome(enum.Enum):
    """The one result each ref gets."""

    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    ERROR = "ERROR"


@dataclasses.dataclass(frozen=True)
class Ref:
    """One image reference on the registry, ``ghcr.io/<path>:<tag>``."""

    path: str
    tag: str

    def __str__(self) -> str:
        return f"{REGISTRY}/{self.path}:{self.tag}"


@dataclasses.dataclass(frozen=True)
class LookupResult:
    """What the registry said about one ref.

    ``detail`` says why the lookup failed and is empty unless ``outcome`` is
    ``ERROR``. A status is ``None`` when that request was not made or got no
    answer. ``error_code`` and ``error_message`` are registry text, already
    cleaned for printing. ``digest`` is either a valid ``sha256:`` digest (64
    lowercase hex characters) or empty.
    """

    ref: Ref
    outcome: Outcome
    detail: str = ""
    token_status: int | None = None
    manifest_status: int | None = None
    error_code: str = ""
    error_message: str = ""
    digest: str = ""


@dataclasses.dataclass(frozen=True)
class Decision:
    """The ``check`` decision: ``publish``, ``skip`` or ``fail``."""

    decision: str
    reason: str
    exit_code: int


class InputError(Exception):
    """An input failed the allowlist. The message names the field only."""

    def __init__(self, field: str) -> None:
        super().__init__(field)
        self.field = field


class Transport(Protocol):
    """Makes one GET request."""

    def get(
        self, url: str, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, Mapping[str, str], bytes]:
        """Return ``(status, headers, body)`` for any HTTP answer, or raise."""
        ...


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse every redirect, so a 3xx comes back as an error status."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> None:
        """Never follow a redirect."""
        return None


class UrllibTransport:
    """The real transport: urllib, redirects refused, bodies capped at 64 KiB."""

    def __init__(self) -> None:
        self._opener = urllib.request.build_opener(NoRedirectHandler())

    def get(
        self, url: str, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, Mapping[str, str], bytes]:
        """Send one GET; an HTTP error status is returned, not raised."""
        request = urllib.request.Request(url, headers=dict(headers), method="GET")
        try:
            with self._opener.open(request, timeout=timeout) as response:
                status: int = response.status
                response_headers: Message = response.headers
                body: bytes = response.read(MAX_BODY_BYTES)
                return status, dict(response_headers.items()), body
        except urllib.error.HTTPError as error:
            with error:
                return (
                    error.code,
                    dict(error.headers.items()),
                    error.read(MAX_BODY_BYTES),
                )


def clean_text(text: str) -> str:
    """Make untrusted text safe to print, at most 200 characters.

    Keeps only ASCII letters, digits, spaces and ``._,:;()/=+'-``; every other
    character is dropped. Then writes ``already published`` and
    ``half-published`` (any case) with an underscore, so registry text never
    repeats the script's own outcome wording; finally cuts to 200 characters.
    """
    kept = _UNSAFE_TEXT_RE.sub("", text)
    kept = _ALREADY_PUBLISHED_RE.sub(r"\1_\2", kept)
    kept = _HALF_PUBLISHED_RE.sub(r"\1_\2", kept)
    return kept[:MAX_TEXT_CHARS]


def _valid_digest(value: str) -> str:
    return value if _DIGEST_RE.fullmatch(value) else ""


def _parse_json(body: bytes) -> object:
    try:
        return cast(object, json.loads(body[:MAX_BODY_BYTES]))
    except (ValueError, RecursionError):
        return None


def _as_object(value: object) -> dict[str, object] | None:
    return cast("dict[str, object]", value) if isinstance(value, dict) else None


def _errors_list(body: bytes) -> list[object] | None:
    data = _as_object(_parse_json(body))
    if data is None:
        return None
    errors = data.get("errors")
    return cast("list[object]", errors) if isinstance(errors, list) else None


def _error_fields(body: bytes) -> tuple[str, str]:
    """The first registry error's code and message, cleaned; empty when absent."""
    errors = _errors_list(body)
    if not errors:
        return "", ""
    entry = _as_object(errors[0])
    if entry is None:
        return "", ""
    code = entry.get("code")
    message = entry.get("message")
    return (
        clean_text(code) if isinstance(code, str) else "",
        clean_text(message) if isinstance(message, str) else "",
    )


def _is_manifest_unknown(body: bytes) -> bool:
    """True only for the one 404 body that means the tag is missing."""
    errors = _errors_list(body)
    if errors is None or len(errors) != 1:
        return False
    entry = _as_object(errors[0])
    if entry is None:
        return False
    return (
        entry.get("code") == "MANIFEST_UNKNOWN"
        and entry.get("message") == "manifest unknown"
    )


def _registry_token(body: bytes) -> str | None:
    data = _as_object(_parse_json(body))
    if data is None:
        return None
    token = data.get("token")
    if isinstance(token, str) and _HEADER_TOKEN_RE.fullmatch(token):
        return token
    return None


def _header(headers: Mapping[str, str], name: str) -> str:
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return ""


def _code_suffix(code: str, message: str) -> str:
    return f" {code} ({message})" if code or message else ""


def basic_authorization(env: Mapping[str, str]) -> str | None:
    """The token request's Basic header value, or None when either variable is unset."""
    user = env.get("GHCR_LOOKUP_USER", "")
    token = env.get("GHCR_LOOKUP_TOKEN", "")
    if not user or not token:
        return None
    return "Basic " + base64.b64encode(f"{user}:{token}".encode()).decode("ascii")


def lookup(
    ref: Ref,
    transport: Transport,
    authorization: str | None,
    *,
    anonymous: bool = False,
) -> LookupResult:
    """Ask the registry whether ``ref`` exists: PRESENT, ABSENT or ERROR.

    ``authorization`` is the Basic header value for the token request, or None
    when the workflow token is not set. With ``anonymous`` no header is sent.
    """
    if anonymous:
        token_headers: dict[str, str] = {}
    elif authorization is None:
        return LookupResult(ref, Outcome.ERROR, detail=TOKEN_NOT_SET)
    else:
        token_headers = {"Authorization": authorization}

    token_url = (
        f"https://{REGISTRY}/token?service={REGISTRY}&scope=repository:{ref.path}:pull"
    )
    try:
        token_status, _, token_body = transport.get(
            token_url, token_headers, TIMEOUT_SECONDS
        )
    except _NETWORK_ERRORS as error:
        return LookupResult(ref, Outcome.ERROR, detail=clean_text(type(error).__name__))

    registry_token = _registry_token(token_body) if token_status == 200 else None
    if registry_token is None:
        if token_status == 200:
            return LookupResult(
                ref,
                Outcome.ERROR,
                detail="token HTTP 200 without a usable token",
                token_status=token_status,
            )
        code, message = _error_fields(token_body)
        return LookupResult(
            ref,
            Outcome.ERROR,
            detail=clean_text(
                f"token HTTP {token_status}{_code_suffix(code, message)}"
            ),
            token_status=token_status,
            error_code=code,
            error_message=message,
        )

    manifest_url = f"https://{REGISTRY}/v2/{ref.path}/manifests/{ref.tag}"
    manifest_headers = {"Authorization": f"Bearer {registry_token}", "Accept": ACCEPT}
    try:
        status, headers, body = transport.get(
            manifest_url, manifest_headers, TIMEOUT_SECONDS
        )
    except _NETWORK_ERRORS as error:
        return LookupResult(
            ref,
            Outcome.ERROR,
            detail=clean_text(type(error).__name__),
            token_status=token_status,
        )

    if status == 200:
        return LookupResult(
            ref,
            Outcome.PRESENT,
            token_status=token_status,
            manifest_status=status,
            digest=_valid_digest(_header(headers, "Docker-Content-Digest")),
        )
    code, message = _error_fields(body)
    if status == 404 and _is_manifest_unknown(body):
        return LookupResult(
            ref,
            Outcome.ABSENT,
            token_status=token_status,
            manifest_status=status,
            error_code=code,
            error_message=message,
        )
    return LookupResult(
        ref,
        Outcome.ERROR,
        detail=clean_text(f"manifest HTTP {status}{_code_suffix(code, message)}"),
        token_status=token_status,
        manifest_status=status,
        error_code=code,
        error_message=message,
    )


def _status_text(status: int | None) -> str:
    return "-" if status is None else str(status)


def format_lookup(result: LookupResult) -> str:
    """The log line printed for every lookup."""
    line = (
        f"{result.ref}: token HTTP {_status_text(result.token_status)}; "
        f"manifest HTTP {_status_text(result.manifest_status)}"
        f"{_code_suffix(result.error_code, result.error_message)}"
        f" -> {result.outcome.value}"
    )
    if result.digest:
        line += f" {result.digest}"
    return line


def lookup_failed_reason(result: LookupResult) -> str:
    """The ``check`` reason for a failed lookup."""
    return (
        f"registry lookup failed for {result.ref}: {result.detail}; "
        f"nothing was pushed. {LOOKUP_FAILED_TAIL}"
    )


def decide(results: Sequence[LookupResult], own_files_changed: bool) -> Decision:
    """Decide from every ref's result; the first matching rule wins."""
    if not results:
        raise ValueError("decide() needs at least one lookup result")
    version = results[0].ref.tag
    if any(result.ref.tag != version for result in results):
        raise ValueError("lookup results mix versions")

    for result in results:
        if result.outcome is Outcome.ERROR:
            return Decision("fail", lookup_failed_reason(result), 1)
    present = [str(r.ref) for r in results if r.outcome is Outcome.PRESENT]
    missing = [str(r.ref) for r in results if r.outcome is Outcome.ABSENT]
    if not present:
        return Decision("publish", f"version {version} is not published yet", 0)
    if missing:
        return Decision(
            "fail",
            f"version {version} is half-published (present: {', '.join(present)}; "
            f"missing: {', '.join(missing)}); bump the version",
            1,
        )
    if own_files_changed:
        return Decision(
            "fail", f"version {version} is already published, bump the version", 1
        )
    return Decision("skip", f"version {version} is already published, skipped", 0)


def parse_bool(value: str, field: str) -> bool:
    """Accept exactly ``true`` or ``false``."""
    if value == "true":
        return True
    if value == "false":
        return False
    raise InputError(field)


def parse_archs(value: str) -> list[str]:
    """A JSON list of one or more unique architectures, each aarch64 or amd64."""
    try:
        data = cast(object, json.loads(value))
    except (ValueError, RecursionError):
        raise InputError("--archs") from None
    if not isinstance(data, list):
        raise InputError("--archs")
    archs: list[str] = []
    for item in cast("list[object]", data):
        if not isinstance(item, str) or item not in ARCHITECTURES or item in archs:
            raise InputError("--archs")
        archs.append(item)
    if not archs:
        raise InputError("--archs")
    return archs


def parse_ref(value: str) -> Ref:
    """Split ``--ref`` at the last ``:`` after the last ``/`` and check each part."""
    colon = value.rfind(":")
    if colon <= value.rfind("/"):
        raise InputError("--ref (tag)")
    host, slash, path = value[:colon].partition("/")
    if host != REGISTRY or not slash:
        raise InputError("--ref (registry)")
    if not REPOSITORY_RE.fullmatch(path):
        raise InputError("--ref (repository path)")
    tag = value[colon + 1 :]
    if not TAG_RE.fullmatch(tag):
        raise InputError("--ref (tag)")
    return Ref(path, tag)


def build_refs(
    registry_prefix: str, image_name: str, version: str, archs: Sequence[str]
) -> list[Ref]:
    """Every architecture's ref in ``archs`` order, then the manifest ref."""
    host, slash, owner = registry_prefix.partition("/")
    if host != REGISTRY or not slash or not _NAME_RE.fullmatch(owner):
        raise InputError("--registry-prefix")
    if not _NAME_RE.fullmatch(image_name):
        raise InputError("--image-name")
    if not TAG_RE.fullmatch(version):
        raise InputError("--version")
    refs = [Ref(f"{owner}/{arch}-{image_name}", version) for arch in archs]
    refs.append(Ref(f"{owner}/{image_name}", version))
    for ref in refs:
        if not REPOSITORY_RE.fullmatch(ref.path):
            raise InputError("--registry-prefix or --image-name")
    return refs


def _emit(line: str) -> None:
    # Nothing printed can contain a line break, so nothing can start a workflow
    # command except the script's own prefixes.
    if not _SINGLE_LINE_RE.fullmatch(line):
        raise ValueError("refusing to print a line break")
    print(line, flush=True)


def _summary_cell(text: str) -> str:
    return text.replace("|", "\\|") if text else "-"


def _summary(results: Sequence[LookupResult], decision_line: str) -> str:
    rows = [
        "| Ref | HTTP status | Error code | Result |",
        "| --- | --- | --- | --- |",
    ]
    for result in results:
        status = (
            f"token {_status_text(result.token_status)}, "
            f"manifest {_status_text(result.manifest_status)}"
        )
        rows.append(
            f"| {result.ref} | {status} | {_summary_cell(result.error_code)} "
            f"| {result.outcome.value} |"
        )
    return "\n".join(rows) + "\n\n" + decision_line + "\n"


def _run_check(
    args: argparse.Namespace, env: Mapping[str, str], transport: Transport
) -> int:
    archs = parse_archs(args.archs)
    refs = build_refs(args.registry_prefix, args.image_name, args.version, archs)
    own_files_changed = parse_bool(args.own_files_changed, "--own-files-changed")
    publish_mode = parse_bool(args.publish_mode, "--publish-mode")
    anonymous: bool = args.anonymous
    authorization = basic_authorization(env)

    results: list[LookupResult] = []
    for ref in refs:
        result = lookup(ref, transport, authorization, anonymous=anonymous)
        _emit(format_lookup(result))
        results.append(result)

    decision = decide(results, own_files_changed)
    push = publish_mode and decision.decision == "publish"
    outputs = {
        "decision": decision.decision,
        "reason": decision.reason,
        "push": "true" if push else "false",
    }
    for key, value in outputs.items():
        if not _SINGLE_LINE_RE.fullmatch(value):
            raise ValueError(f"output {key} is not a single line")

    decision_line = f"publish-guard decision: {decision.decision} ({decision.reason})"
    _emit(decision_line)
    if decision.decision == "fail":
        _emit(f"::error::{decision.reason}")
    elif decision.decision == "skip":
        _emit(f"::notice::{decision.reason}")

    output_path = env.get("GITHUB_OUTPUT", "")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as handle:
            handle.writelines(f"{key}={value}\n" for key, value in outputs.items())
    summary_path = env.get("GITHUB_STEP_SUMMARY", "")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as handle:
            handle.write(_summary(results, decision_line))
    return decision.exit_code


def _run_tag(
    args: argparse.Namespace, env: Mapping[str, str], transport: Transport
) -> int:
    ref = parse_ref(args.ref)
    push = parse_bool(args.push, "--push")
    anonymous: bool = args.anonymous
    result = lookup(ref, transport, basic_authorization(env), anonymous=anonymous)
    _emit(format_lookup(result))
    if result.outcome is Outcome.ERROR:
        _emit(
            f"{ref}: registry lookup failed: {result.detail}; "
            f"nothing was pushed. {LOOKUP_FAILED_TAIL}"
        )
        return 1
    if result.outcome is Outcome.PRESENT and push:
        _emit(f"{ref} already exists, stopping before push; bump the version")
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    """The command line: ``check`` and ``tag``."""
    parser = argparse.ArgumentParser(
        prog="publish_guard.py",
        description="Check the registry before an add-on release pushes anything.",
        allow_abbrev=False,
    )
    modes = parser.add_subparsers(dest="mode", required=True, metavar="{check,tag}")
    anonymous_help = "send no credentials (local runs only; workflows never pass it)"

    check = modes.add_parser(
        "check",
        help="look up every tag of a version and decide publish, skip or fail",
        description=(
            "Look up every architecture's tag, then the manifest tag, and decide "
            "publish, skip or fail."
        ),
        allow_abbrev=False,
    )
    check.add_argument(
        "--registry-prefix",
        required=True,
        help="registry and owner, for example ghcr.io/example",
    )
    check.add_argument(
        "--image-name", required=True, help="image name, for example addon"
    )
    check.add_argument(
        "--version", required=True, help="version tag, for example 1.2.3"
    )
    check.add_argument(
        "--archs",
        required=True,
        help='JSON list of architectures, for example ["aarch64","amd64"]',
    )
    check.add_argument(
        "--own-files-changed",
        required=True,
        help="true or false: the add-on's own files changed",
    )
    check.add_argument(
        "--publish-mode",
        required=True,
        help="true or false: this run publishes",
    )
    check.add_argument("--anonymous", action="store_true", help=anonymous_help)

    tag = modes.add_parser(
        "tag",
        help="re-check the one tag a job is about to push",
        description="Re-check the one tag a job is about to push.",
        allow_abbrev=False,
    )
    tag.add_argument(
        "--ref",
        required=True,
        help="exact ref, for example ghcr.io/example/amd64-addon:1.2.3",
    )
    tag.add_argument(
        "--push",
        required=True,
        help="true: this job is about to push, so a present tag stops it; "
        "false: report only",
    )
    tag.add_argument("--anonymous", action="store_true", help=anonymous_help)
    return parser


def main(argv: Sequence[str], env: Mapping[str, str], transport: Transport) -> int:
    """Run one mode and return the exit code.

    Every input is checked before any request; an invalid input exits 1 and
    names the field, never the value.
    """
    args = build_parser().parse_args(list(argv))
    try:
        if args.mode == "check":
            return _run_check(args, env, transport)
        return _run_tag(args, env, transport)
    except InputError as error:
        _emit(f"::error::invalid value for {error.field}; nothing was pushed")
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], os.environ, UrllibTransport()))
