"""Wiring tests: read the workflow YAML and assert on its literal shape.

These tests execute only two things: the ``init`` job's bash filter script
(with synthetic app names and file paths), and ``publish_guard.main`` against
the same fake transport pattern used in ``test_publish_guard.py``. They never
call GitHub's own expression evaluator; W8 is explicit about that limit.

PyYAML parses the ``on:`` key as the boolean ``True``, so tests that need it
read ``wf[True]``.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest
import yaml

import publish_guard as pg

REPO_ROOT = Path(__file__).resolve().parents[3]
BUILDER_PATH = REPO_ROOT / ".github" / "workflows" / "builder.yaml"
BUILD_APP_PATH = REPO_ROOT / ".github" / "workflows" / "build-app.yaml"


def _load(path: Path) -> dict[Any, Any]:
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


@pytest.fixture()
def builder() -> dict[Any, Any]:
    return _load(BUILDER_PATH)


@pytest.fixture()
def build_app() -> dict[Any, Any]:
    return _load(BUILD_APP_PATH)


def _step(steps: list[dict[Any, Any]], step_id: str) -> dict[Any, Any]:
    for step in steps:
        if step.get("id") == step_id:
            return step
    raise AssertionError(f"no step with id {step_id!r}")


def _collapsed(run: str) -> str:
    return " ".join(run.split())


def _collect_uses(node: Any) -> list[str]:
    found: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "uses" and isinstance(value, str):
                found.append(value)
            else:
                found.extend(_collect_uses(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_collect_uses(item))
    return found


def _collect_run(node: Any) -> list[str]:
    found: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "run" and isinstance(value, str):
                found.append(value)
            else:
                found.extend(_collect_run(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_collect_run(item))
    return found


def _flatten_strings(node: Any) -> list[str]:
    found: list[str] = []
    if isinstance(node, dict):
        for value in node.values():
            found.extend(_flatten_strings(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_flatten_strings(item))
    elif isinstance(node, str):
        found.append(node)
    return found


# --------------------------------------------------------------------------
# W1
# --------------------------------------------------------------------------


def test_w1_guard_step_in_prepare(build_app: dict[Any, Any]) -> None:
    prepare = build_app["jobs"]["prepare"]
    outputs = prepare["outputs"]
    assert outputs["decision"] == "${{ steps.guard.outputs.decision }}"
    assert outputs["reason"] == "${{ steps.guard.outputs.reason }}"
    assert outputs["push"] == "${{ steps.guard.outputs.push }}"

    steps = prepare["steps"]
    guard_index = next(i for i, s in enumerate(steps) if s.get("id") == "guard")
    matrix_index = next(i for i, s in enumerate(steps) if s.get("id") == "matrix")
    assert guard_index > matrix_index

    guard = steps[guard_index]
    assert "if" not in guard
    assert "continue-on-error" not in guard
    assert guard["env"] == {
        "GHCR_LOOKUP_TOKEN": "${{ secrets.GITHUB_TOKEN }}",
        "GHCR_LOOKUP_USER": "${{ github.repository_owner }}",
        "REGISTRY_PREFIX": "${{ steps.normalize.outputs.registry_prefix }}",
        "IMAGE_NAME": "${{ steps.normalize.outputs.image_name }}",
        "VERSION": "${{ steps.normalize.outputs.version }}",
        "ARCHS": "${{ steps.info.outputs.architectures }}",
        "OWN_FILES_CHANGED": "${{ inputs.own-files-changed }}",
        "PUBLISH_MODE": "${{ inputs.publish }}",
    }
    assert _collapsed(guard["run"]) == (
        'python3 .github/scripts/publish_guard.py check --registry-prefix "$REGISTRY_PREFIX" '
        '--image-name "$IMAGE_NAME" --version "$VERSION" --archs "$ARCHS" '
        '--own-files-changed "$OWN_FILES_CHANGED" --publish-mode "$PUBLISH_MODE"'
    )


# --------------------------------------------------------------------------
# W2 / W3
# --------------------------------------------------------------------------


def test_w2_build_image_push_gate(build_app: dict[Any, Any]) -> None:
    build_job = build_app["jobs"]["build"]
    build_image_step = next(
        s
        for s in build_job["steps"]
        if s.get("uses", "").startswith("home-assistant/builder/actions/build-image@")
    )
    assert (
        build_image_step["with"]["push"]
        == "${{ inputs.publish && needs.prepare.outputs.push == 'true' }}"
    )


def test_w3_manifest_gate_and_needs(build_app: dict[Any, Any]) -> None:
    manifest_job = build_app["jobs"]["manifest"]
    assert (
        manifest_job["if"] == "inputs.publish && needs.prepare.outputs.push == 'true'"
    )
    assert manifest_job["needs"] == ["prepare", "build"]


# --------------------------------------------------------------------------
# W4 / W5
# --------------------------------------------------------------------------


def test_w4_build_recheck_order_and_shape(build_app: dict[Any, Any]) -> None:
    steps = build_app["jobs"]["build"]["steps"]
    checkout, recheck, build_image = steps[0], steps[1], steps[2]

    assert checkout.get("uses", "").startswith("actions/checkout@")
    assert "uses" not in recheck
    assert _collapsed(recheck["run"]) == (
        'python3 .github/scripts/publish_guard.py tag --ref "$REF" --push "$PUSH"'
    )
    assert build_image.get("uses", "").startswith(
        "home-assistant/builder/actions/build-image@"
    )

    assert recheck["env"]["PUSH"] == (
        "${{ inputs.publish && needs.prepare.outputs.push == 'true' }}"
    )
    assert (
        recheck["env"]["REF"]
        == "${{ matrix.image }}:${{ needs.prepare.outputs.version }}"
    )
    assert "if" not in recheck
    assert "continue-on-error" not in recheck


def test_w5_manifest_recheck_order_and_shape(build_app: dict[Any, Any]) -> None:
    steps = build_app["jobs"]["manifest"]["steps"]
    checkout, recheck, publish_step = steps[0], steps[1], steps[2]

    assert (
        checkout["uses"] == "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd"
    )
    assert checkout["with"]["persist-credentials"] is False

    assert "uses" not in recheck
    assert _collapsed(recheck["run"]) == (
        'python3 .github/scripts/publish_guard.py tag --ref "$REF" --push true'
    )
    assert recheck["env"]["REF"] == (
        "${{ needs.prepare.outputs.registry_prefix }}/${{ needs.prepare.outputs.image_name }}"
        ":${{ needs.prepare.outputs.version }}"
    )
    assert "if" not in recheck
    assert "continue-on-error" not in recheck

    assert publish_step.get("uses", "").startswith(
        "home-assistant/builder/actions/publish-multi-arch-manifest@"
    )


# --------------------------------------------------------------------------
# W6
# --------------------------------------------------------------------------


def test_w6_image_tags_drop_latest(build_app: dict[Any, Any]) -> None:
    jobs = build_app["jobs"]
    build_image_step = next(
        s
        for s in jobs["build"]["steps"]
        if s.get("uses", "").startswith("home-assistant/builder/actions/build-image@")
    )
    manifest_publish_step = next(
        s
        for s in jobs["manifest"]["steps"]
        if s.get("uses", "").startswith(
            "home-assistant/builder/actions/publish-multi-arch-manifest@"
        )
    )
    for step in (build_image_step, manifest_publish_step):
        tags = [
            line.strip()
            for line in step["with"]["image-tags"].splitlines()
            if line.strip()
        ]
        assert tags == ["${{ needs.prepare.outputs.version }}"]


# --------------------------------------------------------------------------
# W7
# --------------------------------------------------------------------------

EXPECTED_USES = {
    "actions/checkout@v6.0.2",
    "tj-actions/changed-files@24d32ffd492484c1d75e0c0b894501ddb9d30d62",
    "home-assistant/actions/helpers/find-addons@f4ca6f671bd429efb108c0f2fa0ae8af0215986c",
    "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd",
    "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1",
    "./.github/workflows/build-app.yaml",
    "home-assistant/actions/helpers/info@master",
    "home-assistant/builder/actions/prepare-multi-arch-matrix@2026.03.2",
    "home-assistant/builder/actions/build-image@2026.03.2",
    "home-assistant/builder/actions/publish-multi-arch-manifest@2026.03.2",
}

SHA_CHECKOUT = "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd"
SHA_SETUP_PYTHON = "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1"


def _jobs_using(workflow: dict[Any, Any], use: str) -> set[str]:
    return {name for name, job in workflow["jobs"].items() if use in _collect_uses(job)}


FORBIDDEN_RUN_SUBSTRINGS = (
    "docker push",
    "docker login",
    "imagetools",
    "crane ",
    "skopeo",
    "oras ",
)


def test_w7_uses_allowlist_and_run_denylist(
    builder: dict[Any, Any], build_app: dict[Any, Any]
) -> None:
    all_uses = _collect_uses(builder) + _collect_uses(build_app)
    assert set(all_uses) == EXPECTED_USES

    publish_guard_tests = builder["jobs"]["publish-guard-tests"]

    assert _jobs_using(builder, SHA_SETUP_PYTHON) == {"publish-guard-tests"}
    assert _jobs_using(build_app, SHA_SETUP_PYTHON) == set()
    assert _jobs_using(builder, SHA_CHECKOUT) == {"publish-guard-tests"}
    assert _jobs_using(build_app, SHA_CHECKOUT) == {"manifest"}

    build_image_count = sum(
        1
        for use in all_uses
        if use == "home-assistant/builder/actions/build-image@2026.03.2"
    )
    assert build_image_count == 1
    manifest_count = sum(
        1
        for use in all_uses
        if use == "home-assistant/builder/actions/publish-multi-arch-manifest@2026.03.2"
    )
    assert manifest_count == 1

    for path in sorted((REPO_ROOT / ".github" / "workflows").glob("*.y*ml")):
        workflow = _load(path)
        for run_text in _collect_run(workflow):
            for forbidden in FORBIDDEN_RUN_SUBSTRINGS:
                assert forbidden not in run_text, (path, forbidden)

    assert publish_guard_tests["permissions"] == {"contents": "read"}
    assert "if" not in publish_guard_tests
    assert "outputs" not in publish_guard_tests
    assert not any("secrets." in s for s in _flatten_strings(publish_guard_tests))


# --------------------------------------------------------------------------
# W8
# --------------------------------------------------------------------------


class _FakeTransport:
    def __init__(self) -> None:
        self._responses: dict[str, tuple[int, dict[str, str], bytes]] = {}

    def set(self, url: str, response: tuple[int, dict[str, str], bytes]) -> None:
        self._responses[url] = response

    def get(
        self, url: str, headers: Any, timeout: float
    ) -> tuple[int, dict[str, str], bytes]:
        return self._responses[url]


def _token_url(path: str) -> str:
    return f"https://ghcr.io/token?service=ghcr.io&scope=repository:{path}:pull"


def _manifest_url(path: str, tag: str) -> str:
    return f"https://ghcr.io/v2/{path}/manifests/{tag}"


def _set_present(transport: _FakeTransport, ref: pg.Ref) -> None:
    transport.set(
        _token_url(ref.path),
        (200, {}, json.dumps({"token": "fake-registry-pull-token"}).encode()),
    )
    transport.set(_manifest_url(ref.path, ref.tag), (200, {}, b"{}"))


_W8_ENV = {
    "GHCR_LOOKUP_USER": "example-owner",
    "GHCR_LOOKUP_TOKEN": "fake-workflow-token-w8",
}


def test_w8_all_present_bulk_run_pushes_nothing(
    tmp_path: Path, build_app: dict[Any, Any]
) -> None:
    build_image_step = next(
        s
        for s in build_app["jobs"]["build"]["steps"]
        if s.get("uses", "").startswith("home-assistant/builder/actions/build-image@")
    )
    manifest_job = build_app["jobs"]["manifest"]
    assert (
        build_image_step["with"]["push"]
        == "${{ inputs.publish && needs.prepare.outputs.push == 'true' }}"
    )
    assert (
        manifest_job["if"] == "inputs.publish && needs.prepare.outputs.push == 'true'"
    )

    transport = _FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    for ref in refs:
        _set_present(transport, ref)

    output_path = tmp_path / "github_output"
    output_path.write_text("")
    env = dict(_W8_ENV, GITHUB_OUTPUT=str(output_path))

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
        env,
        transport,
    )
    assert exit_code == 0
    outputs = dict(line.split("=", 1) for line in output_path.read_text().splitlines())
    assert outputs["decision"] == "skip"
    assert outputs["push"] == "false"

    # This models the two gate expressions above (W2, W3) for one known set of
    # GitHub Actions inputs; it is not GitHub's own evaluator.
    inputs_publish = True
    prepare_push = outputs["push"] == "true"
    build_push = inputs_publish and prepare_push
    manifest_runs = inputs_publish and prepare_push
    assert build_push is False
    assert manifest_runs is False


def test_w8_own_files_changed_run_fails_prepare(tmp_path: Path) -> None:
    transport = _FakeTransport()
    refs = pg.build_refs("ghcr.io/example", "addon", "1.2.3", ["aarch64", "amd64"])
    for ref in refs:
        _set_present(transport, ref)

    output_path = tmp_path / "github_output"
    output_path.write_text("")
    env = dict(_W8_ENV, GITHUB_OUTPUT=str(output_path))

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
            "true",
            "--publish-mode",
            "true",
        ],
        env,
        transport,
    )
    # prepare's own job fails; build and manifest never start.
    assert exit_code == 1


# --------------------------------------------------------------------------
# W9 / W10
# --------------------------------------------------------------------------


def test_w9_build_app_caller(builder: dict[Any, Any]) -> None:
    build_app_job = builder["jobs"]["build-app"]
    assert build_app_job["concurrency"] == {
        "group": (
            "${{ github.event_name == 'pull_request' && "
            "format('publish-guard-pr-{0}-{1}', github.event.pull_request.number, matrix.app) || "
            "format('publish-guard-release-{0}', matrix.app) }}"
        ),
        "cancel-in-progress": False,
        "queue": "max",
    }
    assert build_app_job["with"]["own-files-changed"] == (
        "${{ contains(fromJSON(needs.init.outputs.own_changed_apps), matrix.app) }}"
    )
    assert build_app_job["with"]["publish"] == (
        "${{ github.event_name == 'push' || github.event_name == 'workflow_dispatch' }}"
    )
    assert builder["jobs"]["init"]["outputs"]["own_changed_apps"] == (
        "${{ steps.filter.outputs.own_changed_apps }}"
    )
    assert build_app_job["needs"] == ["init", "publish-guard-tests"]
    assert build_app_job["if"] == "needs.init.outputs.changed == 'true'"


def test_w10_own_files_changed_input(build_app: dict[Any, Any]) -> None:
    assert build_app[True]["workflow_call"]["inputs"]["own-files-changed"] == {
        "required": True,
        "type": "boolean",
    }


# --------------------------------------------------------------------------
# W11
# --------------------------------------------------------------------------


def _run_filter_script(
    script: str, monitored_files: str, event_name: str, changed_files: str
) -> dict[str, object]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        output_path = Path(tmp_dir) / "github_output"
        output_path.write_text("")
        env = dict(os.environ)
        env.update(
            {
                "APPS": "addon-a addon-b",
                "CHANGED_FILES": changed_files,
                "EVENT_NAME": event_name,
                "MONITORED_FILES": monitored_files,
                "GITHUB_OUTPUT": str(output_path),
            }
        )
        completed = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-c", script],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr

        result: dict[str, object] = {}
        for line in output_path.read_text().splitlines():
            key, _, value = line.partition("=")
            if key in ("own_changed_apps", "changed_apps"):
                result[key] = json.loads(value)
            else:
                result[key] = value
        if "changed" in result:
            result["changed"] = result["changed"] == "true"
        return result


W11_CASES = [
    pytest.param(
        "workflow_dispatch",
        "addon-a/config.yaml",
        [],
        True,
        ["addon-a", "addon-b"],
        id="workflow-dispatch",
    ),
    pytest.param(
        "push",
        ".github/workflows/builder.yaml addon-a/config.yaml",
        ["addon-a"],
        True,
        ["addon-a", "addon-b"],
        id="push-workflow-edit-plus-own-change",
    ),
    pytest.param(
        "push",
        ".github/workflows/build-app.yaml",
        [],
        True,
        ["addon-a", "addon-b"],
        id="push-build-app-workflow-edit",
    ),
    pytest.param(
        "pull_request",
        ".github/scripts/publish_guard.py",
        [],
        True,
        ["addon-a", "addon-b"],
        id="pr-script-edit",
    ),
    pytest.param(
        "push",
        "addon-b/Dockerfile",
        ["addon-b"],
        True,
        ["addon-b"],
        id="push-own-change-only",
    ),
    pytest.param("push", "README.md", [], False, None, id="push-unrelated-file"),
    pytest.param(
        "push",
        ".github/scripts/tests/test_publish_guard.py",
        [],
        False,
        None,
        id="push-test-file-only",
    ),
]


@pytest.mark.parametrize(
    (
        "event_name",
        "changed_files",
        "expected_own",
        "expected_changed",
        "expected_changed_apps",
    ),
    W11_CASES,
)
def test_w11_filter_step_cases(
    builder: dict[Any, Any],
    event_name: str,
    changed_files: str,
    expected_own: list[str],
    expected_changed: bool,
    expected_changed_apps: list[str] | None,
) -> None:
    filter_step = _step(builder["jobs"]["init"]["steps"], "filter")
    script = filter_step["run"]
    monitored_files = builder["env"]["MONITORED_FILES"]

    result = _run_filter_script(script, monitored_files, event_name, changed_files)

    assert result["own_changed_apps"] == expected_own
    assert result["changed"] == expected_changed
    if expected_changed_apps is None:
        assert "changed_apps" not in result
    else:
        assert result["changed_apps"] == expected_changed_apps


# --------------------------------------------------------------------------
# W12 / W13
# --------------------------------------------------------------------------


def test_w12_no_probe_name_or_anonymous_flag() -> None:
    for path in sorted((REPO_ROOT / ".github" / "workflows").glob("*.y*ml")):
        text = path.read_text(encoding="utf-8")
        assert "publish-guard-probe" not in text
        assert "--anonymous" not in text


def test_w13_run_steps_have_no_expression_syntax(build_app: dict[Any, Any]) -> None:
    prepare_guard = _step(build_app["jobs"]["prepare"]["steps"], "guard")
    build_recheck = build_app["jobs"]["build"]["steps"][1]
    manifest_recheck = build_app["jobs"]["manifest"]["steps"][1]
    for step in (prepare_guard, build_recheck, manifest_recheck):
        assert "${{" not in step["run"]


# --------------------------------------------------------------------------
# W14
# --------------------------------------------------------------------------


def test_w14_publish_guard_tests_steps(builder: dict[Any, Any]) -> None:
    job = builder["jobs"]["publish-guard-tests"]
    steps = job["steps"]
    run_steps = [step for step in steps if "run" in step]
    assert [_collapsed(step["run"]) for step in run_steps] == [
        "pip install pytest==8.4.1 pyyaml==6.0.3 ruff==0.15.13 pyright==1.1.409",
        "ruff check --target-version py312 .github/scripts",
        "ruff format --check .github/scripts",
        "pyright --pythonversion 3.12 publish_guard.py",
        "PYTHONPATH=.github/scripts python -m pytest -p no:cacheprovider .github/scripts/tests",
    ]
    assert run_steps[3]["working-directory"] == ".github/scripts"
    assert "continue-on-error" not in job
    for step in steps:
        assert "if" not in step and "continue-on-error" not in step
