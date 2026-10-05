"""Unit tests for ``publish_guard.py``, against a scripted fake registry.

Every test drives ``publish_guard.main`` (or a smaller seam: ``decide``,
``build_refs``, ``NoRedirectHandler``) through the module's own test seams
(``main(argv, env, transport)``). No test makes a real network call; the
autouse fixture in ``conftest.py`` blocks sockets outright, and the fakes
here answer every request from a script instead.

All registry values (owner, image name, version, tokens) are synthetic.
"""

from __future__ import annotations

import base64
import json
import socket
from collections.abc import Mapping
from itertools import combinations
from pathlib import Path
from typing import Any

import pytest

import publish_guard as pg

BASE_ENV: dict[str, str] = {
    "GHCR_LOOKUP_USER": "example-owner",
    "GHCR_LOOKUP_TOKEN": "fake-workflow-token-0001",
}
TOKEN_VALUE = "fake-registry-pull-token-0002"

LOOKUP_FAILED_TAIL = (
    "Re-run only after a timeout, connection error, 429 or 5xx; other answers "
    'usually repeat, see "When a lookup fails" in publish_guard.py'
)
TOKEN_NOT_SET = "workflow token not set"


def token_url(path: str) -> str:
    return f"https://ghcr.io/token?service=ghcr.io&scope=repository:{path}:pull"


def manifest_url(path: str, tag: str) -> str:
    return f"https://ghcr.io/v2/{path}/manifests/{tag}"


class FakeTransport:
    """A scriptable fake for ``publish_guard.Transport``.

    Responses are keyed by exact URL. ``set`` can be called again on the
    same URL to change the answer mid-test (see
    ``test_same_run_completion_sequence``).
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str], float]] = []
        self._responses: dict[
            str, tuple[int, dict[str, str], bytes] | BaseException
        ] = {}

    def set(
        self, url: str, response: tuple[int, dict[str, str], bytes] | BaseException
    ) -> None:
        self._responses[url] = response

    def get(
        self, url: str, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, Mapping[str, str], bytes]:
        self.calls.append((url, dict(headers), timeout))
        if url not in self._responses:
            raise AssertionError(f"no scripted fake response for {url!r}")
        response = self._responses[url]
        if isinstance(response, BaseException):
            raise response
        return response


class NoCallTransport:
    """A transport that fails the test if it is ever called.

    Used for invalid-input cases, where the script must fail before making
    any request.
    """

    def get(
        self, url: str, headers: Mapping[str, str], timeout: float
    ) -> tuple[int, Mapping[str, str], bytes]:
        raise AssertionError(f"unexpected network call to {url!r}")


def set_token_ok(transport: FakeTransport, ref: pg.Ref) -> None:
    transport.set(
        token_url(ref.path), (200, {}, json.dumps({"token": TOKEN_VALUE}).encode())
    )


def set_present(transport: FakeTransport, ref: pg.Ref, digest: str = "") -> None:
    set_token_ok(transport, ref)
    headers = {"Docker-Content-Digest": digest} if digest else {}
    transport.set(manifest_url(ref.path, ref.tag), (200, headers, b"{}"))


def set_absent(transport: FakeTransport, ref: pg.Ref) -> None:
    set_token_ok(transport, ref)
    body = json.dumps(
        {"errors": [{"code": "MANIFEST_UNKNOWN", "message": "manifest unknown"}]}
    ).encode()
    transport.set(manifest_url(ref.path, ref.tag), (404, {}, body))


def set_manifest_error(
    transport: FakeTransport, ref: pg.Ref, status: int, body: bytes = b""
) -> None:
    set_token_ok(transport, ref)
    transport.set(manifest_url(ref.path, ref.tag), (status, {}, body))


def run_check(
    output_dir: Path,
    transport: FakeTransport | NoCallTransport,
    *,
    registry_prefix: str = "ghcr.io/example",
    image_name: str = "addon",
    version: str = "1.2.3",
    archs: str = '["aarch64","amd64"]',
    own_files_changed: str = "false",
    publish_mode: str = "true",
    anonymous: bool = False,
    env: Mapping[str, str] | None = None,
    use_output: bool = True,
    use_summary: bool = False,
) -> tuple[int, dict[str, str], Path | None, Path | None]:
    argv = [
        "check",
        "--registry-prefix",
        registry_prefix,
        "--image-name",
        image_name,
        "--version",
        version,
        "--archs",
        archs,
        "--own-files-changed",
        own_files_changed,
        "--publish-mode",
        publish_mode,
    ]
    if anonymous:
        argv.append("--anonymous")

    run_env: dict[str, str] = dict(BASE_ENV if env is None else env)
    output_path: Path | None = None
    if use_output:
        output_path = output_dir / "github_output"
        output_path.write_text("")
        run_env["GITHUB_OUTPUT"] = str(output_path)
    summary_path: Path | None = None
    if use_summary:
        summary_path = output_dir / "github_summary"
        summary_path.write_text("")
        run_env["GITHUB_STEP_SUMMARY"] = str(summary_path)

    exit_code = pg.main(argv, run_env, transport)

    outputs: dict[str, str] = {}
    if output_path is not None:
        for line in output_path.read_text().splitlines():
            key, _, value = line.partition("=")
            outputs[key] = value
    return exit_code, outputs, output_path, summary_path


# --------------------------------------------------------------------------
# 5.1 The proposal's test list
# --------------------------------------------------------------------------


@pytest.mark.parametrize("own", [True, False], ids=["own", "bulk"])
@pytest.mark.parametrize(
    "publish_mode", [True, False], ids=["publish-mode-true", "publish-mode-false"]
)
def test_all_absent_publishes(tmp_path: Path, own: bool, publish_mode: bool) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    for ref in refs:
        set_absent(transport, ref)

    exit_code, outputs, _, _ = run_check(
        tmp_path,
        transport,
        own_files_changed="true" if own else "false",
        publish_mode="true" if publish_mode else "false",
    )

    assert exit_code == 0
    assert outputs["decision"] == "publish"
    assert outputs["push"] == ("true" if publish_mode else "false")


def test_all_present_own_change_fails_with_bump(tmp_path: Path) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    for ref in refs:
        set_present(transport, ref)

    exit_code, outputs, _, _ = run_check(
        tmp_path, transport, own_files_changed="true", publish_mode="true"
    )

    assert exit_code == 1
    assert outputs["decision"] == "fail"
    assert "bump the version" in outputs["reason"]
    assert outputs["push"] == "false"


def test_all_present_bulk_skips(tmp_path: Path) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    for ref in refs:
        set_present(transport, ref)

    exit_code, outputs, _, _ = run_check(
        tmp_path, transport, own_files_changed="false", publish_mode="true"
    )

    assert exit_code == 0
    assert outputs["decision"] == "skip"
    assert outputs["push"] == "false"


ALL_REF_NAMES = ("aarch64", "amd64", "manifest")


def _half_published_subsets() -> list[tuple[str, ...]]:
    subsets: list[tuple[str, ...]] = []
    for size in (1, 2):
        subsets.extend(combinations(ALL_REF_NAMES, size))
    return subsets


@pytest.mark.parametrize(
    "present_names", _half_published_subsets(), ids=lambda s: "+".join(s)
)
@pytest.mark.parametrize("own", [True, False], ids=["own", "bulk"])
def test_half_published_fails(
    tmp_path: Path, present_names: tuple[str, ...], own: bool
) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    ref_by_name = {"aarch64": refs[0], "amd64": refs[1], "manifest": refs[2]}
    for name, ref in ref_by_name.items():
        if name in present_names:
            set_present(transport, ref)
        else:
            set_absent(transport, ref)

    exit_code, outputs, _, _ = run_check(
        tmp_path,
        transport,
        own_files_changed="true" if own else "false",
        publish_mode="true",
    )

    assert exit_code == 1
    assert outputs["decision"] == "fail"
    assert "half-published" in outputs["reason"]
    assert "bump the version" in outputs["reason"]
    assert outputs["push"] == "false"


def test_present_plus_server_error_is_lookup_error(tmp_path: Path) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    set_present(transport, refs[0])
    set_manifest_error(transport, refs[1], 503)
    set_present(transport, refs[2])

    exit_code, outputs, _, _ = run_check(
        tmp_path, transport, own_files_changed="false", publish_mode="true"
    )

    assert exit_code == 1
    assert outputs["decision"] == "fail"
    assert outputs["reason"].startswith("registry lookup failed")
    assert "already published" not in outputs["reason"]
    assert "half-published" not in outputs["reason"]


def _apply_manifest_status(status: int) -> Any:
    def _apply(transport: FakeTransport, ref: pg.Ref) -> None:
        set_manifest_error(transport, ref, status)

    return _apply


def _apply_manifest_timeout() -> Any:
    def _apply(transport: FakeTransport, ref: pg.Ref) -> None:
        set_token_ok(transport, ref)
        transport.set(manifest_url(ref.path, ref.tag), TimeoutError("fake timeout"))

    return _apply


def _apply_manifest_connection_error() -> Any:
    def _apply(transport: FakeTransport, ref: pg.Ref) -> None:
        set_token_ok(transport, ref)
        transport.set(
            manifest_url(ref.path, ref.tag), ConnectionError("fake connection error")
        )

    return _apply


def _apply_token_status(status: int) -> Any:
    def _apply(transport: FakeTransport, ref: pg.Ref) -> None:
        transport.set(token_url(ref.path), (status, {}, b""))

    return _apply


def _apply_token_body_not_json() -> Any:
    def _apply(transport: FakeTransport, ref: pg.Ref) -> None:
        transport.set(token_url(ref.path), (200, {}, b"not json"))

    return _apply


def _apply_token_field_missing() -> Any:
    def _apply(transport: FakeTransport, ref: pg.Ref) -> None:
        transport.set(token_url(ref.path), (200, {}, b"{}"))

    return _apply


def _apply_manifest_404_body(body_obj: object) -> Any:
    def _apply(transport: FakeTransport, ref: pg.Ref) -> None:
        set_token_ok(transport, ref)
        body = b"" if body_obj is None else json.dumps(body_obj).encode()
        transport.set(manifest_url(ref.path, ref.tag), (404, {}, body))

    return _apply


def _apply_manifest_404_html() -> Any:
    def _apply(transport: FakeTransport, ref: pg.Ref) -> None:
        set_token_ok(transport, ref)
        transport.set(
            manifest_url(ref.path, ref.tag), (404, {}, b"<html>not found</html>")
        )

    return _apply


LOOKUP_ERROR_CASES = [
    pytest.param(_apply_manifest_status(401), id="manifest-401"),
    pytest.param(_apply_manifest_status(403), id="manifest-403"),
    pytest.param(_apply_manifest_status(500), id="manifest-500"),
    pytest.param(_apply_manifest_status(502), id="manifest-502"),
    pytest.param(_apply_manifest_status(503), id="manifest-503"),
    pytest.param(_apply_manifest_status(302), id="manifest-302"),
    pytest.param(_apply_manifest_timeout(), id="manifest-timeout"),
    pytest.param(_apply_manifest_connection_error(), id="manifest-connection-error"),
    pytest.param(_apply_token_status(401), id="token-401"),
    pytest.param(_apply_token_status(403), id="token-403"),
    pytest.param(_apply_token_status(500), id="token-500"),
    pytest.param(_apply_token_body_not_json(), id="token-body-not-json"),
    pytest.param(_apply_token_field_missing(), id="token-field-missing"),
    pytest.param(
        _apply_manifest_404_body(
            {
                "errors": [
                    {
                        "code": "NAME_UNKNOWN",
                        "message": "repository name not known to registry",
                    }
                ]
            }
        ),
        id="404-name-unknown",
    ),
    pytest.param(
        _apply_manifest_404_body(
            {
                "errors": [
                    {
                        "code": "MANIFEST_UNKNOWN",
                        "message": (
                            "OCI index found, but Accept header does not "
                            "support OCI indexes"
                        ),
                    }
                ]
            }
        ),
        id="404-wrong-accept-message",
    ),
    pytest.param(_apply_manifest_404_body(None), id="404-empty-body"),
    pytest.param(_apply_manifest_404_html(), id="404-html-body"),
]


@pytest.mark.parametrize("apply_error", LOOKUP_ERROR_CASES)
def test_lookup_errors_fail_without_published_wording(
    tmp_path: Path, apply_error: Any
) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["amd64"])
    apply_error(transport, refs[0])
    set_absent(transport, refs[1])

    exit_code, outputs, _, _ = run_check(
        tmp_path,
        transport,
        archs='["amd64"]',
        own_files_changed="false",
        publish_mode="true",
    )

    assert exit_code == 1
    assert outputs["decision"] == "fail"
    assert outputs["reason"].startswith("registry lookup failed for ")
    assert outputs["reason"].endswith(LOOKUP_FAILED_TAIL)


def test_tag_require_absent_stops_when_present(
    capsys: pytest.CaptureFixture[str],
) -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    set_present(transport, ref)

    exit_code = pg.main(
        ["tag", "--ref", str(ref), "--push", "true"], BASE_ENV, transport
    )

    assert exit_code == 1
    out = capsys.readouterr().out
    assert "already exists, stopping before push; bump the version" in out


def test_same_run_completion_sequence() -> None:
    transport = FakeTransport()
    aarch64_ref = pg.Ref("example/aarch64-addon", "1.2.3")
    amd64_ref = pg.Ref("example/amd64-addon", "1.2.3")
    manifest_ref = pg.Ref("example/addon", "1.2.3")

    set_present(transport, amd64_ref)
    set_absent(transport, aarch64_ref)
    set_absent(transport, manifest_ref)

    exit_code = pg.main(
        ["tag", "--ref", str(aarch64_ref), "--push", "true"], BASE_ENV, transport
    )
    assert exit_code == 0

    set_present(transport, aarch64_ref)

    exit_code = pg.main(
        ["tag", "--ref", str(manifest_ref), "--push", "true"], BASE_ENV, transport
    )
    assert exit_code == 0

    exit_code = pg.main(
        ["tag", "--ref", str(amd64_ref), "--push", "true"], BASE_ENV, transport
    )
    assert exit_code == 1


@pytest.mark.parametrize("own", [True, False], ids=["own", "bulk"])
def test_other_run_on_half_published_is_blocked(tmp_path: Path, own: bool) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    set_present(transport, refs[0])
    set_absent(transport, refs[1])
    set_absent(transport, refs[2])

    exit_code, outputs, _, _ = run_check(
        tmp_path,
        transport,
        own_files_changed="true" if own else "false",
        publish_mode="true",
    )

    assert exit_code == 1
    assert outputs["decision"] == "fail"
    assert "half-published" in outputs["reason"]
    assert outputs["push"] == "false"


def _apply_state(transport: FakeTransport, refs: list[pg.Ref], state: object) -> None:
    ref_by_name = {"aarch64": refs[0], "amd64": refs[1], "manifest": refs[2]}
    if state == "all-absent":
        for ref in refs:
            set_absent(transport, ref)
    elif state == "all-present":
        for ref in refs:
            set_present(transport, ref)
    elif state == "error":
        set_manifest_error(transport, refs[0], 503)
        for ref in refs[1:]:
            set_absent(transport, ref)
    else:
        names = state
        for name, ref in ref_by_name.items():
            if name in names:  # type: ignore[operator]
                set_present(transport, ref)
            else:
                set_absent(transport, ref)


PR_MODE_STATES = [
    pytest.param("all-absent", id="all-absent"),
    pytest.param("all-present", id="all-present"),
    pytest.param(frozenset({"aarch64"}), id="half-aarch64"),
    pytest.param(frozenset({"amd64"}), id="half-amd64"),
    pytest.param(frozenset({"manifest"}), id="half-manifest"),
    pytest.param(frozenset({"aarch64", "amd64"}), id="half-aarch64-amd64"),
    pytest.param(frozenset({"aarch64", "manifest"}), id="half-aarch64-manifest"),
    pytest.param(frozenset({"amd64", "manifest"}), id="half-amd64-manifest"),
    pytest.param("error", id="error"),
]


@pytest.mark.parametrize("state", PR_MODE_STATES)
def test_pr_mode_matches_release_mode_and_never_pushes(
    tmp_path: Path, state: object
) -> None:
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])

    release_dir = tmp_path / "release"
    release_dir.mkdir()
    release_transport = FakeTransport()
    _apply_state(release_transport, refs, state)
    _, release_outputs, _, _ = run_check(
        release_dir, release_transport, own_files_changed="false", publish_mode="true"
    )

    pr_dir = tmp_path / "pr"
    pr_dir.mkdir()
    pr_transport = FakeTransport()
    _apply_state(pr_transport, refs, state)
    _, pr_outputs, _, _ = run_check(
        pr_dir, pr_transport, own_files_changed="false", publish_mode="false"
    )

    assert pr_outputs["decision"] == release_outputs["decision"]
    assert pr_outputs["push"] == "false"
    if release_outputs["decision"] == "publish":
        assert release_outputs["push"] == "true"


# --------------------------------------------------------------------------
# 5.2 Further unit tests
# --------------------------------------------------------------------------


def test_accept_header_is_exact_four_type_set() -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    set_present(transport, ref)

    pg.main(["tag", "--ref", str(ref), "--push", "false"], BASE_ENV, transport)

    _, headers, _ = transport.calls[-1]
    accept_types = set(headers["Accept"].split(","))
    assert accept_types == {
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }


def test_token_request_url_and_basic_auth() -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    set_present(transport, ref)

    pg.main(["tag", "--ref", str(ref), "--push", "false"], BASE_ENV, transport)

    token_call = transport.calls[0]
    assert token_call[0] == (
        "https://ghcr.io/token?service=ghcr.io&scope=repository:example/amd64-addon:pull"
    )
    expected_basic = "Basic " + base64.b64encode(
        f"{BASE_ENV['GHCR_LOOKUP_USER']}:{BASE_ENV['GHCR_LOOKUP_TOKEN']}".encode()
    ).decode("ascii")
    assert token_call[1]["Authorization"] == expected_basic


def test_manifest_request_uses_registry_bearer_token() -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    set_present(transport, ref)

    pg.main(["tag", "--ref", str(ref), "--push", "false"], BASE_ENV, transport)

    manifest_call = transport.calls[-1]
    assert manifest_call[0] == manifest_url(ref.path, ref.tag)
    assert manifest_call[1]["Authorization"] == f"Bearer {TOKEN_VALUE}"


def test_missing_workflow_token_is_lookup_error(tmp_path: Path) -> None:
    transport = NoCallTransport()

    exit_code, outputs, _, _ = run_check(
        tmp_path,
        transport,
        env={},
        own_files_changed="false",
        publish_mode="true",
    )

    assert exit_code == 1
    assert outputs["reason"].startswith("registry lookup failed for ")
    assert TOKEN_NOT_SET in outputs["reason"]
    assert outputs["reason"].endswith(LOOKUP_FAILED_TAIL)


def test_anonymous_mode_sends_no_authorization() -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    set_absent(transport, ref)

    exit_code = pg.main(
        ["tag", "--ref", str(ref), "--push", "false", "--anonymous"], {}, transport
    )

    assert exit_code == 0
    token_call = transport.calls[0]
    assert "Authorization" not in token_call[1]


ABSENT_BODY_CASES = [
    pytest.param(
        {"errors": [{"code": "MANIFEST_UNKNOWN", "message": "manifest unknown"}]},
        True,
        id="exact-single-entry",
    ),
    pytest.param(
        {
            "errors": [
                {"code": "MANIFEST_UNKNOWN", "message": "manifest unknown"},
                {"code": "MANIFEST_UNKNOWN", "message": "manifest unknown"},
            ]
        },
        False,
        id="two-entries",
    ),
    pytest.param(
        {"errors": [{"code": "MANIFEST_UNKNOWN", "message": "a different wording"}]},
        False,
        id="different-message",
    ),
    pytest.param(
        {
            "errors": [
                {"code": "MANIFEST_UNKNOWN", "message": "manifest unknown"},
                {
                    "code": "NAME_UNKNOWN",
                    "message": "repository name not known to registry",
                },
                {"code": "UNAUTHORIZED", "message": "access denied"},
            ]
        },
        False,
        id="extra-entries",
    ),
]


@pytest.mark.parametrize(("body", "expect_absent"), ABSENT_BODY_CASES)
def test_absent_needs_exact_registry_body(
    body: dict[str, object], expect_absent: bool
) -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    set_token_ok(transport, ref)
    transport.set(manifest_url(ref.path, ref.tag), (404, {}, json.dumps(body).encode()))

    exit_code = pg.main(
        ["tag", "--ref", str(ref), "--push", "true"], BASE_ENV, transport
    )

    assert exit_code == (0 if expect_absent else 1)


def test_refs_follow_builder_naming() -> None:
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    assert [str(ref) for ref in refs] == [
        "ghcr.io/example/aarch64-addon:1.2.3",
        "ghcr.io/example/amd64-addon:1.2.3",
        "ghcr.io/example/addon:1.2.3",
    ]

    reordered = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["amd64", "aarch64"])
    assert [str(ref) for ref in reordered] == [
        "ghcr.io/example/amd64-addon:1.2.3",
        "ghcr.io/example/aarch64-addon:1.2.3",
        "ghcr.io/example/addon:1.2.3",
    ]


_CHECK_TAIL = [
    "--registry-prefix",
    "ghcr.io/example",
    "--image-name",
    "addon",
    "--version",
    "1.2.3",
    "--archs",
    '["aarch64"]',
    "--own-files-changed",
    "true",
    "--publish-mode",
    "true",
]


def _check_tail_with(**overrides: str) -> list[str]:
    tail = dict(zip(_CHECK_TAIL[0::2], _CHECK_TAIL[1::2]))
    tail.update(overrides)
    result: list[str] = []
    for key, value in tail.items():
        result.extend((key, value))
    return result


INVALID_INPUT_CASES = [
    pytest.param(
        "check",
        _check_tail_with(**{"--registry-prefix": "docker.io/example"}),
        id="host-other-than-ghcr",
    ),
    pytest.param(
        "check",
        _check_tail_with(**{"--registry-prefix": "ghcr.io/Example"}),
        id="uppercase-path",
    ),
    pytest.param(
        "tag", ["--ref", "ghcr.io/addon:1.2.3", "--push", "true"], id="one-segment-path"
    ),
    pytest.param(
        "check", _check_tail_with(**{"--version": "1.2\n3"}), id="version-newline"
    ),
    pytest.param(
        "check", _check_tail_with(**{"--version": "1.2 3"}), id="version-space"
    ),
    pytest.param(
        "check", _check_tail_with(**{"--version": "1.2:3"}), id="version-colon"
    ),
    pytest.param(
        "check", _check_tail_with(**{"--version": "a" * 129}), id="version-129-chars"
    ),
    pytest.param("check", _check_tail_with(**{"--archs": "[]"}), id="empty-archs"),
    pytest.param(
        "check", _check_tail_with(**{"--archs": '["riscv"]'}), id="unknown-arch"
    ),
    pytest.param(
        "check",
        _check_tail_with(**{"--archs": '["amd64","amd64"]'}),
        id="duplicated-arch",
    ),
    pytest.param(
        "check", _check_tail_with(**{"--archs": "not-json"}), id="non-json-archs"
    ),
    pytest.param(
        "check",
        _check_tail_with(**{"--own-files-changed": "True"}),
        id="boolean-capitalized",
    ),
    pytest.param(
        "check", _check_tail_with(**{"--own-files-changed": "1"}), id="boolean-one"
    ),
    pytest.param(
        "check", _check_tail_with(**{"--own-files-changed": ""}), id="boolean-empty"
    ),
]


@pytest.mark.parametrize(("mode", "tail"), INVALID_INPUT_CASES)
def test_invalid_input_fails_closed(mode: str, tail: list[str]) -> None:
    transport = NoCallTransport()
    exit_code = pg.main([mode, *tail], BASE_ENV, transport)
    assert exit_code == 1


def test_outputs_are_single_line(tmp_path: Path) -> None:
    transport = FakeTransport()
    bad_ref, other_ref, manifest_ref = pg.build_refs(
        "ghcr.io/example", "addon", "1.2.3", ["amd64", "aarch64"]
    )
    set_token_ok(transport, bad_ref)
    transport.set(
        manifest_url(bad_ref.path, bad_ref.tag),
        (
            404,
            {},
            json.dumps(
                {
                    "errors": [
                        {
                            "code": "MANIFEST_UNKNOWN",
                            "message": "weird\nmultiline\nmessage",
                        }
                    ]
                }
            ).encode(),
        ),
    )
    set_absent(transport, other_ref)
    set_absent(transport, manifest_ref)

    exit_code, _outputs, output_path, _ = run_check(
        tmp_path,
        transport,
        archs='["amd64","aarch64"]',
        own_files_changed="false",
        publish_mode="true",
    )

    assert exit_code == 1
    assert output_path is not None
    raw_bytes = output_path.read_bytes()
    assert b"\r" not in raw_bytes and raw_bytes.endswith(b"\n")
    raw = raw_bytes.decode()
    lines = raw.split("\n")[:-1]
    assert [line.partition("=")[0] for line in lines] == ["decision", "reason", "push"]
    assert lines[2] == "push=false"


def test_runs_without_github_output(capsys: pytest.CaptureFixture[str]) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    for ref in refs:
        set_absent(transport, ref)

    exit_code = pg.main(
        [
            "check",
            "--registry-prefix",
            "ghcr.io/example",
            "--image-name",
            "addon",
            "--version",
            "1.2.3",
            "--archs",
            '["aarch64","amd64"]',
            "--own-files-changed",
            "false",
            "--publish-mode",
            "true",
        ],
        dict(BASE_ENV),
        transport,
    )

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "publish-guard decision: publish" in out


def test_tokens_never_printed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    for ref in refs:
        set_present(transport, ref)

    exit_code, _outputs, output_path, summary_path = run_check(
        tmp_path,
        transport,
        own_files_changed="true",
        publish_mode="true",
        use_summary=True,
    )

    assert exit_code == 1
    captured = capsys.readouterr()
    haystacks = [captured.out, captured.err]
    if output_path is not None:
        haystacks.append(output_path.read_text())
    if summary_path is not None:
        haystacks.append(summary_path.read_text())
    for haystack in haystacks:
        assert BASE_ENV["GHCR_LOOKUP_TOKEN"] not in haystack
        assert TOKEN_VALUE not in haystack


HOSTILE = (
    "x ##[add-mask]m ##[set-output name=push;]true %0A::error::fake"
    "\r\n::warning::fake |pipe| [link](http://e)"
)
BAD_MARKERS = ("##[", "%", "\r", "[", "]", "|")
WHERE = [
    "token-401-message",
    "manifest-500-code",
    "manifest-500-message",
    "manifest-404-message",
    "manifest-200-digest",
]


def _errors(code: str, message: str) -> bytes:
    return json.dumps({"errors": [{"code": code, "message": message}]}).encode()


def _apply_hostile(where: str, transport: FakeTransport, ref: pg.Ref) -> None:
    murl = manifest_url(ref.path, ref.tag)
    if where == "token-401-message":
        transport.set(token_url(ref.path), (401, {}, _errors("DENIED", HOSTILE)))
    elif where == "manifest-500-code":
        set_token_ok(transport, ref)
        transport.set(murl, (500, {}, _errors(HOSTILE, "m")))
    elif where == "manifest-500-message":
        set_token_ok(transport, ref)
        transport.set(murl, (500, {}, _errors("X", HOSTILE)))
    elif where == "manifest-404-message":
        set_token_ok(transport, ref)
        transport.set(murl, (404, {}, _errors("MANIFEST_UNKNOWN", HOSTILE)))
    else:  # manifest-200-digest
        set_present(transport, ref, digest="sha256:" + "a" * 64 + HOSTILE)


def _assert_clean(text: str) -> None:
    for marker in BAD_MARKERS:
        assert marker not in text, marker
    for line in text.split("\n"):
        if line.lstrip().startswith("::"):
            assert line.startswith(("::error::", "::notice::")), line


@pytest.mark.parametrize("where", WHERE)
def test_check_registry_text_cannot_carry_commands(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], where: str
) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    for i, ref in enumerate(refs):
        if where == "manifest-200-digest" or i == 0:
            _apply_hostile(where, transport, ref)
        else:
            set_absent(transport, ref)
    _code, _outputs, output_path, summary_path = run_check(
        tmp_path, transport, use_summary=True
    )
    out = capsys.readouterr().out
    assert sum(1 for line in out.split("\n") if line.startswith("::")) <= 1
    _assert_clean(out)
    assert output_path is not None and summary_path is not None
    raw_bytes = output_path.read_bytes()
    assert b"\r" not in raw_bytes and raw_bytes.endswith(b"\n")
    raw = raw_bytes.decode()
    assert [line.partition("=")[0] for line in raw.split("\n")[:-1]] == [
        "decision",
        "reason",
        "push",
    ]
    _assert_clean(raw)
    summary = summary_path.read_text()  # the table itself uses "|"
    assert "##[" not in summary and "%" not in summary


@pytest.mark.parametrize("where", WHERE)
def test_tag_registry_text_cannot_carry_commands(
    capsys: pytest.CaptureFixture[str], where: str
) -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    _apply_hostile(where, transport, ref)
    pg.main(["tag", "--ref", str(ref), "--push", "false"], BASE_ENV, transport)
    _assert_clean(capsys.readouterr().out)


def test_valid_digest_is_printed(capsys: pytest.CaptureFixture[str]) -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    digest = "sha256:" + "0123456789abcdef" * 4
    set_present(transport, ref, digest=digest)
    assert (
        pg.main(["tag", "--ref", str(ref), "--push", "false"], BASE_ENV, transport) == 0
    )
    assert capsys.readouterr().out.splitlines()[0].endswith(f"-> PRESENT {digest}")


def test_malformed_digest_with_only_safe_characters_is_dropped(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Every character here already passes clean_text's own filter, so only a
    # real sha256-shape check (not character cleaning) can reject it.
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    set_present(transport, ref, digest="sha256:" + "a" * 63 + "Z")
    assert (
        pg.main(["tag", "--ref", str(ref), "--push", "false"], BASE_ENV, transport) == 0
    )
    line = capsys.readouterr().out.splitlines()[0]
    assert line.endswith("-> PRESENT")
    assert "sha256:" not in line


def test_clean_text_truncates() -> None:
    assert len(pg.clean_text("y" * 300)) == 200


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("already #published", "already_published", id="filter-first"),
        pytest.param("half-#published", "half_published", id="filter-first-half"),
        pytest.param("Already  Published", "Already_Published", id="two-spaces"),
    ],
)
def test_clean_text_filters_before_rewriting(raw: str, expected: str) -> None:
    assert pg.clean_text(raw) == expected


def test_message_constants_match_spec() -> None:
    assert pg.LOOKUP_FAILED_TAIL == LOOKUP_FAILED_TAIL
    assert pg.TOKEN_NOT_SET == TOKEN_NOT_SET


@pytest.mark.parametrize("which", ["emit-guard", "output-guard"])
def test_line_break_guards_stop_before_any_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    which: str,
) -> None:
    monkeypatch.setattr(pg, "clean_text", lambda t: t)
    if which == "output-guard":
        monkeypatch.setattr(pg, "_emit", print)

    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["amd64"])
    set_manifest_error(transport, refs[0], 500, _errors("X", "a\nb"))
    set_absent(transport, refs[1])

    output_path = tmp_path / "github_output"
    output_path.write_text("")
    env = dict(BASE_ENV, GITHUB_OUTPUT=str(output_path))

    with pytest.raises(ValueError):
        pg.main(
            [
                "check",
                "--registry-prefix",
                "ghcr.io/example",
                "--image-name",
                "addon",
                "--version",
                "1.2.3",
                "--archs",
                '["amd64"]',
                "--own-files-changed",
                "false",
                "--publish-mode",
                "true",
            ],
            env,
            transport,
        )

    if which == "emit-guard":
        assert capsys.readouterr().out == ""
    assert output_path.read_bytes() == b""


def test_registry_phrases_are_scrubbed(tmp_path: Path) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["amd64"])
    set_manifest_error(
        transport, refs[0], 500, _errors("X", "Already Published; HALF-published")
    )
    set_absent(transport, refs[1])

    _code, outputs, _, _ = run_check(tmp_path, transport, archs='["amd64"]')
    assert "already published" not in outputs["reason"].lower()
    assert "half-published" not in outputs["reason"].lower()


def test_first_error_ref_is_named(tmp_path: Path) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    set_absent(transport, refs[0])
    set_manifest_error(transport, refs[1], 500)
    set_manifest_error(transport, refs[2], 503)

    _code, outputs, _, _ = run_check(tmp_path, transport)
    assert outputs["reason"].startswith(f"registry lookup failed for {refs[1]}: ")
    assert str(refs[2]) not in outputs["reason"]


UNUSABLE_TOKEN_VALUES = ["", "a b", "tok\n", "tok\r\nX-Evil: 1", "tök", "\x7f"]


@pytest.mark.parametrize("token", UNUSABLE_TOKEN_VALUES)
def test_unusable_registry_token_is_lookup_error(tmp_path: Path, token: str) -> None:
    transport = FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["amd64"])
    transport.set(
        token_url(refs[0].path),
        (200, {}, json.dumps({"token": token}).encode()),
    )
    set_absent(transport, refs[1])

    exit_code, outputs, _, _ = run_check(tmp_path, transport, archs='["amd64"]')

    assert exit_code == 1
    assert outputs["reason"].startswith(f"registry lookup failed for {refs[0]}: ")
    called_urls = [call[0] for call in transport.calls]
    assert manifest_url(refs[0].path, refs[0].tag) not in called_urls


def test_lookup_failure_help_present() -> None:
    assert pg.__doc__ is not None
    assert "When a lookup fails" in pg.__doc__


def test_tag_require_absent_passes_when_absent() -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    set_absent(transport, ref)

    exit_code = pg.main(
        ["tag", "--ref", str(ref), "--push", "true"], BASE_ENV, transport
    )

    assert exit_code == 0


def test_tag_require_absent_fails_on_error(capsys: pytest.CaptureFixture[str]) -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    set_manifest_error(transport, ref, 503)

    exit_code = pg.main(
        ["tag", "--ref", str(ref), "--push", "true"], BASE_ENV, transport
    )

    assert exit_code == 1
    out = capsys.readouterr().out
    assert "registry lookup failed:" in out
    assert out.rstrip("\n").splitlines()[-1].endswith(LOOKUP_FAILED_TAIL)


@pytest.mark.parametrize(
    ("outcome", "expected_exit"),
    [
        pytest.param("present", 0, id="200-report"),
        pytest.param("absent", 0, id="404-report"),
        pytest.param("error", 1, id="error-report"),
    ],
)
def test_tag_report_mode(
    capsys: pytest.CaptureFixture[str], outcome: str, expected_exit: int
) -> None:
    transport = FakeTransport()
    ref = pg.Ref("example/amd64-addon", "1.2.3")
    if outcome == "present":
        set_present(transport, ref)
    elif outcome == "absent":
        set_absent(transport, ref)
    else:
        set_manifest_error(transport, ref, 503)

    exit_code = pg.main(
        ["tag", "--ref", str(ref), "--push", "false"], BASE_ENV, transport
    )

    assert exit_code == expected_exit
    if outcome == "error":
        out = capsys.readouterr().out
        assert "registry lookup failed:" in out
        assert out.rstrip("\n").splitlines()[-1].endswith(LOOKUP_FAILED_TAIL)


def test_no_redirect_handler_refuses() -> None:
    handler = pg.NoRedirectHandler()
    result = handler.redirect_request(
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        302,
        "Found",
        None,  # type: ignore[arg-type]
        "https://ghcr.io/elsewhere",
    )
    assert result is None


def test_network_is_blocked() -> None:
    with pytest.raises(AssertionError):
        socket.create_connection(("ghcr.io", 443))
