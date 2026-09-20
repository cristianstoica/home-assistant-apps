"""Citation-anchor guard: no ``<name>.py:<digits>`` in production prose.

A comment or docstring that cites a file plus a line number is wrong the
moment the cited file changes above that line, and nothing says so. The
durable rule is to cite the owning symbol instead. This module enforces the
*form* of that rule only: it rejects explicit line-number anchors in the
comments and docstrings of production Python (``wxverify/`` and
``scripts/``), and never checks that a cited symbol or line exists.

The scan is token- and AST-based rather than textual: comments are
``tokenize.COMMENT`` tokens, docstrings are the ``STRING`` tokens whose span
lies inside an AST docstring expression, and every other string token
(log format strings, f-strings, URLs in code) is never looked at, so it can
never false-positive. ``tests/`` is outside the population by construction,
not by an exemption marker.
"""

from __future__ import annotations

import ast
import io
import re
import textwrap
import tokenize
from dataclasses import dataclass
from pathlib import Path

import jinja2
import pytest

from wxverify.web import render

_REPO = Path(__file__).resolve().parents[1]
_PYTHON_ROOTS = ("wxverify", "scripts")

#: A citation anchor. The lookbehind refuses a start preceded by a word
#: character, ``.``, ``/``, ``:``, ``@`` or ``-`` (URL paths, ``user@host``,
#: mid-word starts); ``\w\.py`` demands a name before ``.py``; ``[-:]\d+``
#: folds ranges and ``line:col``; the tail folds ``, :<digits>``
#: continuations, across a newline inside a docstring, into one finding.
#: There is no trailing boundary on purpose.
_ANCHOR = re.compile(
    r"(?<![\w./:@-])[\w./-]*\w\.py:\d+(?:[-:]\d+)*(?:\s*,\s*:\d+(?:-\d+)?)*"
)

_MESSAGE = (
    "Line-number citations found in production docstrings/comments. Cite the "
    "owning symbol instead (e.g. 'see _decide_precip in decision.py'); line "
    "numbers drift the moment the file changes."
)


@dataclass(frozen=True)
class _Finding:
    path: str  # repo-relative POSIX, e.g. "wxverify/verification/decision.py"
    line: int  # 1-based physical line of the anchor's first character
    where: str  # "comment" | "module docstring" | "docstring of <name>"
    text: str  # the matched anchor, internal whitespace collapsed to one space


def _char_col(line: str, byte_col: int) -> int:
    """Convert an AST UTF-8 byte column to the tokenizer's character column."""
    return len(line.encode("utf-8")[:byte_col].decode("utf-8"))


def _docstring_spans(
    source: str, tree: ast.Module
) -> list[tuple[tuple[int, int], tuple[int, int], str]]:
    """Every docstring's ``(start, end, where)`` in tokenizer coordinates.

    A docstring is ``body[0]`` being ``Expr(Constant(str))`` of a ``Module``,
    ``ClassDef``, ``FunctionDef`` or ``AsyncFunctionDef`` -- the predicate
    ``ast.get_docstring`` uses. Columns are converted from the AST's byte
    offsets against the physical lines the tokenizer itself iterates
    (``io.StringIO(...).readlines()`` breaks on ``\\n`` only, unlike
    ``str.splitlines``).
    """
    lines = io.StringIO(source).readlines()
    spans: list[tuple[tuple[int, int], tuple[int, int], str]] = []
    for node in ast.walk(tree):
        if (
            not isinstance(
                node,
                (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
            )
            or not node.body
        ):
            continue
        first = node.body[0]
        if not (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            continue
        assert first.end_lineno is not None and first.end_col_offset is not None
        where = (
            "module docstring"
            if isinstance(node, ast.Module)
            else f"docstring of {node.name}"
        )
        start = (first.lineno, _char_col(lines[first.lineno - 1], first.col_offset))
        end = (
            first.end_lineno,
            _char_col(lines[first.end_lineno - 1], first.end_col_offset),
        )
        spans.append((start, end, where))
    return spans


def _python_findings(source: str, rel_path: str) -> list[_Finding]:
    """Anchors in the comments and docstrings of one module's source.

    Parse errors propagate: ``ast.parse`` is given ``rel_path`` as the
    filename so a ``SyntaxError`` names the file. The anchor's line is
    counted on the raw token text, which is right across backslash
    continuations and implicit concatenation where the string *value* is not.
    """
    tree = ast.parse(source, filename=rel_path)
    spans = _docstring_spans(source, tree)
    findings: list[_Finding] = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.COMMENT:
            where = "comment"
        elif tok.type == tokenize.STRING:
            hit = next(
                (w for s, e, w in spans if s <= tok.start and tok.end <= e), None
            )
            if hit is None:
                continue
            where = hit
        else:
            continue
        for m in _ANCHOR.finditer(tok.string):
            line = tok.start[0] + tok.string[: m.start()].count("\n")
            findings.append(
                _Finding(rel_path, line, where, " ".join(m.group(0).split()))
            )
    return findings


def _production_python_files(repo: Path) -> list[Path]:
    """Every ``.py`` under the allowlisted roots, ``__pycache__`` skipped, sorted.

    ``tests/``, ``rootfs/``, ``.venv/`` and the repo root are excluded by not
    being roots, never by a denylist over a whole-repo walk.
    """
    return sorted(
        p
        for root in _PYTHON_ROOTS
        for p in (repo / root).rglob("*.py")
        if "__pycache__" not in p.parts
    )


def _scan_production(repo: Path) -> list[_Finding]:
    """Findings across the production population under ``repo``, in scan order.

    Sources are read with ``tokenize.open`` so the scanner decodes exactly as
    the interpreter's import path does (UTF-8 BOM and PEP 263 cookie
    honoured). The ``try`` never swallows: it attaches the repo-relative path
    as a note and re-raises the same object, so a ``UnicodeDecodeError`` from
    the read -- which names no file on its own -- is attributable.
    """
    out: list[_Finding] = []
    for p in _production_python_files(repo):
        rel = p.relative_to(repo).as_posix()
        try:
            with tokenize.open(p) as fh:
                source = fh.read()
            out.extend(_python_findings(source, rel))
        except Exception as exc:
            exc.add_note(f"while scanning {rel}")
            raise
    return out


def _format(findings: list[_Finding]) -> str:
    """The gate's failure message: one fixed header, one indented line per finding."""
    lines = [_MESSAGE]
    lines.extend(f"  {f.path}:{f.line} ({f.where}): {f.text}" for f in findings)
    return "\n".join(lines)


def test_no_line_number_citations_in_production_python() -> None:
    findings = _scan_production(_REPO)
    assert not findings, _format(findings)


# O1-O17 below


def test_o1_comment_anchor() -> None:
    findings = _python_findings("x = 1  # see decision.py:881\n", "wxverify/a.py")
    assert findings == [_Finding("wxverify/a.py", 1, "comment", "decision.py:881")]


_O2_CASES: list[tuple[str, list[_Finding]]] = [
    (
        '"""Module.\n\nCited at runs.py:12 here.\n"""\n',
        [_Finding("wxverify/a.py", 3, "module docstring", "runs.py:12")],
    ),
    (
        '"""Line one \\\ncontinues; anchor a.py:1 here\n"""\n',
        [_Finding("wxverify/a.py", 2, "module docstring", "a.py:1")],
    ),
    (
        '# see \x0c note\ndef f():\n    """See a.py:1."""\n',
        [_Finding("wxverify/a.py", 3, "docstring of f", "a.py:1")],
    ),
]


@pytest.mark.parametrize(
    "source, expected",
    _O2_CASES,
    ids=["module-docstring", "backslash-cont", "form-feed"],
)
def test_o2_module_docstring_line_arithmetic(
    source: str, expected: list[_Finding]
) -> None:
    assert _python_findings(source, "wxverify/a.py") == expected


def test_o3_every_docstring_container() -> None:
    source = textwrap.dedent(
        '''\
        def f():
            """See a.py:1."""

        async def g():
            """See b.py:2."""

        class C:
            """See c.py:3."""

            def m(self):
                """See d.py:4."""

        def naïve(): """see e.py:5"""
        '''
    )
    findings = _python_findings(source, "wxverify/a.py")
    assert findings == [
        _Finding("wxverify/a.py", 2, "docstring of f", "a.py:1"),
        _Finding("wxverify/a.py", 5, "docstring of g", "b.py:2"),
        _Finding("wxverify/a.py", 8, "docstring of C", "c.py:3"),
        _Finding("wxverify/a.py", 11, "docstring of m", "d.py:4"),
        _Finding("wxverify/a.py", 13, "docstring of naïve", "e.py:5"),
    ]


def test_o4_ordinary_strings_never_scanned() -> None:
    source = textwrap.dedent(
        """\
        LOG = "job failed at runs.py:12"
        x = f"{name}.py:{lineno}"
        y = f"see runs.py:12"
        z = {"k": "decision.py:881"}
        call("runs.py:12")
        def f():
            return "wxverify/verification/runs.py:12-15"
        """
    )
    assert _python_findings(source, "wxverify/a.py") == []


_O5_COMMENT_CASES: list[tuple[str, str]] = [
    ("x = 1  # decision.py:1155, :1159\n", "decision.py:1155, :1159"),
    (
        "x = 1  # wxverify/verification/runs.py:12-15\n",
        "wxverify/verification/runs.py:12-15",
    ),
    ("x = 1  # runs.py:12:5\n", "runs.py:12:5"),
    ("x = 1  # see ``db/connection.py:131`` here\n", "db/connection.py:131"),
    ("x = 1  # __init__.py:1\n", "__init__.py:1"),
    ("x = 1  # test_foo.py:3, :4, :5\n", "test_foo.py:3, :4, :5"),
]


@pytest.mark.parametrize(
    "source, text",
    _O5_COMMENT_CASES,
    ids=[
        "continuation-tail",
        "path-range",
        "line-col",
        "backticked",
        "dunder-init",
        "multi-continuation",
    ],
)
def test_o5_positive_forms_exact_text(source: str, text: str) -> None:
    assert _python_findings(source, "wxverify/a.py") == [
        _Finding("wxverify/a.py", 1, "comment", text)
    ]


def test_o5_docstring_continuation_across_newline() -> None:
    source = '"""runs.py:12,\n    :15\n"""\n'
    findings = _python_findings(source, "wxverify/a.py")
    assert findings == [
        _Finding("wxverify/a.py", 1, "module docstring", "runs.py:12, :15")
    ]


_O6_NEGATIVES: list[str] = [
    "https://example.com.py:8080/x",
    "http://host.com.py:443/path",
    "foo.pyi:12",
    "foo.py :12",
    "foo.py: 12",
    ".py:12",
    "user@host.py:22",
    "127.0.0.1:8099",
    "localhost:8099",
    "at :479",
    "_shared_basis:987-992",
    "see _order_ladder in decision.py",
    "type: ignore[attr-defined]",
    "noqa: E501",
]


@pytest.mark.parametrize("body", _O6_NEGATIVES)
def test_o6_bounded_regex_negatives(body: str) -> None:
    findings = _python_findings(f"x = 1  # {body}\n", "wxverify/a.py")
    assert findings == []


def test_o6_bounded_regex_positive_control() -> None:
    findings = _python_findings("x = 1  # foo.py:12abc\n", "wxverify/a.py")
    assert findings == [_Finding("wxverify/a.py", 1, "comment", "foo.py:12")]


def test_o7_string_and_comment_on_one_line() -> None:
    findings = _python_findings('x = "runs.py:1"  # runs.py:2\n', "wxverify/a.py")
    assert findings == [_Finding("wxverify/a.py", 1, "comment", "runs.py:2")]


def test_o8_several_anchors_in_one_comment() -> None:
    findings = _python_findings("# runs.py:1 and decision.py:2\n", "wxverify/a.py")
    assert findings == [
        _Finding("wxverify/a.py", 1, "comment", "runs.py:1"),
        _Finding("wxverify/a.py", 1, "comment", "decision.py:2"),
    ]


def test_o8_several_anchors_in_one_docstring() -> None:
    source = '"""\nsee a.py:1\n\nsee b.py:2\n"""\n'
    findings = _python_findings(source, "wxverify/a.py")
    assert findings == [
        _Finding("wxverify/a.py", 2, "module docstring", "a.py:1"),
        _Finding("wxverify/a.py", 4, "module docstring", "b.py:2"),
    ]


def test_o9_enumeration_on_a_temp_tree(tmp_path: Path) -> None:
    (tmp_path / "wxverify").mkdir()
    (tmp_path / "wxverify" / "a.py").write_text("x = 1\n")
    (tmp_path / "wxverify" / "sub" / "__pycache__").mkdir(parents=True)
    (tmp_path / "wxverify" / "sub" / "__pycache__" / "b.py").write_text("x = 1\n")
    (tmp_path / "wxverify" / "c.txt").write_text("x = 1\n")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "c.py").write_text("x = 1\n")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "d.py").write_text("x = 1\n")
    (tmp_path / ".venv" / "lib").mkdir(parents=True)
    (tmp_path / ".venv" / "lib" / "e.py").write_text("x = 1\n")
    (tmp_path / "rootfs").mkdir()
    (tmp_path / "rootfs" / "f.py").write_text("x = 1\n")
    (tmp_path / "g.py").write_text("x = 1\n")

    result = [
        p.relative_to(tmp_path).as_posix() for p in _production_python_files(tmp_path)
    ]
    assert result == ["scripts/c.py", "wxverify/a.py"]


def test_o10_enumeration_on_the_real_repo() -> None:
    for root in _PYTHON_ROOTS:
        assert (_REPO / root).is_dir()

    rel = {p.relative_to(_REPO).as_posix() for p in _production_python_files(_REPO)}
    assert "wxverify/verification/decision.py" in rel
    assert "wxverify/__init__.py" in rel
    assert "scripts/bench_cache_hot_path.py" in rel
    assert not any(r.startswith("tests/") for r in rel)
    assert not any("__pycache__" in Path(r).parts for r in rel)


def test_o11_parse_errors_propagate() -> None:
    with pytest.raises(SyntaxError) as excinfo:
        _python_findings("def f(:\n", "wxverify/bad.py")
    assert excinfo.value.filename == "wxverify/bad.py"


def test_o12_message_format() -> None:
    message = _format(
        [
            _Finding("wxverify/a.py", 3, "comment", "x.py:1"),
            _Finding("wxverify/b.py", 9, "docstring of f", "y.py:2, :3"),
        ]
    )
    assert "cite the owning symbol" in message.lower()
    lines = message.splitlines()
    idx_a = lines.index("  wxverify/a.py:3 (comment): x.py:1")
    idx_b = lines.index("  wxverify/b.py:9 (docstring of f): y.py:2, :3")
    assert idx_a < idx_b


def test_o13_liveness_on_real_production_text() -> None:
    path = _REPO / "wxverify" / "verification" / "decision.py"
    text = path.read_text(encoding="utf-8")
    rel = "wxverify/verification/decision.py"
    assert _python_findings(text, rel) == []
    planted = text + "# planted zzz.py:1\n"
    findings = _python_findings(planted, rel)
    assert findings == [_Finding(rel, text.count("\n") + 1, "comment", "zzz.py:1")]


def test_o14_scan_production_on_a_planted_tree(tmp_path: Path) -> None:
    (tmp_path / "wxverify").mkdir()
    (tmp_path / "scripts").mkdir()
    (tmp_path / "wxverify" / "a.py").write_text("# see zzz.py:1\n")
    (tmp_path / "scripts" / "ok.py").write_text("x = 1\n")

    assert _scan_production(tmp_path) == [
        _Finding("wxverify/a.py", 1, "comment", "zzz.py:1")
    ]

    (tmp_path / "wxverify" / "a.py").write_text("x = 1  # fine\n")
    assert _scan_production(tmp_path) == []


def test_o15_realistic_clean_text() -> None:
    source = textwrap.dedent(
        '''\
        def f() -> None:
            """See ``core.aio.run_to_completion`` and
            ``db.connection.Database._connect_reader``.
            """
            # see load_sites in web/context.py
            pass
        '''
    )
    assert _python_findings(source, "wxverify/a.py") == []


def test_o16_unreadable_file_fails_loudly_undecodable(tmp_path: Path) -> None:
    (tmp_path / "wxverify").mkdir()
    (tmp_path / "wxverify" / "bad.py").write_bytes(b"x = 1\n\x80")
    (tmp_path / "wxverify" / "ok.py").write_text("x = 1\n")

    with pytest.raises(UnicodeDecodeError) as excinfo:
        _scan_production(tmp_path)
    assert "while scanning wxverify/bad.py" in excinfo.value.__notes__


def test_o16_unreadable_file_fails_loudly_syntax_error(tmp_path: Path) -> None:
    (tmp_path / "wxverify").mkdir()
    (tmp_path / "wxverify" / "bad.py").write_text("def f(:\n")
    (tmp_path / "wxverify" / "ok.py").write_text("x = 1\n")

    with pytest.raises(SyntaxError) as excinfo:
        _scan_production(tmp_path)
    assert excinfo.value.filename == "wxverify/bad.py"
    assert "while scanning wxverify/bad.py" in excinfo.value.__notes__


def test_o17_bom_prefixed_file_with_anchor(tmp_path: Path) -> None:
    (tmp_path / "wxverify").mkdir()
    (tmp_path / "scripts").mkdir()
    (tmp_path / "wxverify" / "bom.py").write_bytes(b"\xef\xbb\xbf# see zzz.py:1\n")
    (tmp_path / "scripts" / "ok.py").write_text("x = 1\n")

    assert _scan_production(tmp_path) == [
        _Finding("wxverify/bom.py", 1, "comment", "zzz.py:1")
    ]


def test_o17_bom_prefixed_file_without_anchor(tmp_path: Path) -> None:
    (tmp_path / "wxverify").mkdir()
    (tmp_path / "scripts").mkdir()
    (tmp_path / "wxverify" / "bom.py").write_bytes(b"\xef\xbb\xbfx = 1\n")
    (tmp_path / "scripts" / "ok.py").write_text("x = 1\n")

    assert _scan_production(tmp_path) == []


# --- Jinja templates ---


def _is_template_name(name: str) -> bool:
    """Loader names are '/'-joined; a dot-prefixed component is not a template."""
    return not any(part.startswith(".") for part in name.split("/"))


def _jinja_findings(
    source: str, name: str, env: jinja2.Environment, rel_path: str
) -> list[_Finding]:
    findings: list[_Finding] = []
    # rel_path is the third argument on purpose: it is what a lexer error prints (DB6)
    for lineno, token_type, value in env.lex(source, name, rel_path):
        if token_type != "comment":
            continue
        for m in _ANCHOR.finditer(value):
            line = lineno + value[: m.start()].count("\n")
            findings.append(
                _Finding(rel_path, line, "jinja comment", " ".join(m.group(0).split()))
            )
    return findings


def _template_sources(
    env: jinja2.Environment, root: Path
) -> list[tuple[str, str, str]]:
    """(name, source, repo-relative path) for every template the loader can serve."""
    assert env.loader is not None
    out: list[tuple[str, str, str]] = []
    for name in env.list_templates(filter_func=_is_template_name):
        try:
            source, filename, _ = env.loader.get_source(env, name)
        except Exception as exc:
            exc.add_note(f"while reading template {name}")
            raise
        assert filename is not None
        out.append((name, source, Path(filename).relative_to(root).as_posix()))
    return out


def _template_findings(env: jinja2.Environment, root: Path) -> list[_Finding]:
    out: list[_Finding] = []
    for name, source, rel_path in _template_sources(env, root):
        out.extend(_jinja_findings(source, name, env, rel_path))
    return out


def test_no_line_number_citations_in_templates() -> None:
    findings = _template_findings(render.env, _REPO)
    assert not findings, _format(findings)


# OB1-OB13 below


def _env_tmp(tmp_path: Path) -> jinja2.Environment:
    return render.env.overlay(loader=jinja2.FileSystemLoader(str(tmp_path)))


def test_ob1_one_line_comment(tmp_path: Path) -> None:
    (tmp_path / "t.html").write_text("<p>x</p>{# see decision.py:881 #}\n")
    findings = _template_findings(_env_tmp(tmp_path), tmp_path)
    assert findings == [_Finding("t.html", 1, "jinja comment", "decision.py:881")]


def test_ob2_multi_line_arithmetic(tmp_path: Path) -> None:
    (tmp_path / "t.html").write_text(
        "<p>a</p>\n{# first\n   second decision.py:881\n   third #}\n"
    )
    findings = _template_findings(_env_tmp(tmp_path), tmp_path)
    assert findings == [_Finding("t.html", 3, "jinja comment", "decision.py:881")]


def test_ob3_whitespace_control_with_continuation(tmp_path: Path) -> None:
    (tmp_path / "t.html").write_text("{#- foo.py:3, :4 -#}\n<p>b</p>\n")
    findings = _template_findings(_env_tmp(tmp_path), tmp_path)
    assert findings == [_Finding("t.html", 1, "jinja comment", "foo.py:3, :4")]


def test_ob4_several_anchors_in_one_comment(tmp_path: Path) -> None:
    (tmp_path / "t.html").write_text("{# a.py:1 then b.py:2 #}\n")
    findings = _template_findings(_env_tmp(tmp_path), tmp_path)
    assert findings == [
        _Finding("t.html", 1, "jinja comment", "a.py:1"),
        _Finding("t.html", 1, "jinja comment", "b.py:2"),
    ]


def test_ob4_anchors_across_lines_in_one_comment(tmp_path: Path) -> None:
    (tmp_path / "t.html").write_text("{# a.py:1\nno anchor here\nb.py:2 #}\n")
    findings = _template_findings(_env_tmp(tmp_path), tmp_path)
    assert findings == [
        _Finding("t.html", 1, "jinja comment", "a.py:1"),
        _Finding("t.html", 3, "jinja comment", "b.py:2"),
    ]


def test_ob5_permitted_symbol_references_are_clean(tmp_path: Path) -> None:
    (tmp_path / "t.html").write_text(
        "{# see _decide_precip in decision.py and decide_variable's ladder #}\n"
    )
    findings = _template_findings(_env_tmp(tmp_path), tmp_path)
    assert findings == []


def test_ob6_anchor_like_text_outside_jinja_comments_is_clean(tmp_path: Path) -> None:
    (tmp_path / "t.html").write_text(
        textwrap.dedent(
            """\
            <p>see runs.py:12</p>
            {{ "s.py:8" }}
            {% set x = "b.py:6" %}
            {% raw %}{# c.py:7 #}{% endraw %}
            <!-- d.py:9 -->
            <script>"e.py:10"</script>
            """
        )
    )
    findings = _template_findings(_env_tmp(tmp_path), tmp_path)
    assert findings == []


def test_ob7_scanner_honours_the_given_environment() -> None:
    alt = jinja2.Environment(comment_start_string="<#", comment_end_string="#>")
    assert _jinja_findings("{# foo.py:1 #}", "t", alt, "t.html") == []
    assert _jinja_findings("<# foo.py:1 #>", "t", alt, "t.html") == [
        _Finding("t.html", 1, "jinja comment", "foo.py:1")
    ]
    assert _jinja_findings("{# foo.py:1 #}", "t", render.env, "t.html") == [
        _Finding("t.html", 1, "jinja comment", "foo.py:1")
    ]


def test_ob8_real_loader_enumeration() -> None:
    tdir = _REPO / "wxverify" / "web" / "templates"
    via_loader = {rel for _, _, rel in _template_sources(render.env, _REPO)}
    via_rglob = {
        p.relative_to(_REPO).as_posix()
        for p in tdir.rglob("*")
        if p.is_file() and _is_template_name(p.relative_to(tdir).as_posix())
    }
    assert via_loader == via_rglob
    assert "wxverify/web/templates/verification/show.html" in via_loader
    assert "wxverify/web/templates/base.html" in via_loader
    assert via_loader


def test_ob9_production_gate() -> None:
    test_no_line_number_citations_in_templates()


def test_ob10_template_syntax_error_propagates_and_names_template() -> None:
    with pytest.raises(jinja2.TemplateSyntaxError) as excinfo:
        _jinja_findings("{# never closed", "t", render.env, "t.html")
    assert "t.html" in str(excinfo.value)


def test_ob11_loader_defined_population(tmp_path: Path) -> None:
    (tmp_path / "note.txt").write_text("{# zzz.py:1 #}\n")
    (tmp_path / "page.html").write_text("<p>clean</p>\n")
    findings = _template_findings(_env_tmp(tmp_path), tmp_path)
    assert findings == [_Finding("note.txt", 1, "jinja comment", "zzz.py:1")]


def test_ob12_dot_files_are_skipped_not_decoded(tmp_path: Path) -> None:
    (tmp_path / ".DS_Store").write_bytes(b"\x00\x00\x00\x01Bud1\xff")
    (tmp_path / ".hidden").mkdir()
    (tmp_path / ".hidden" / "x.html").write_text("{# zzz.py:2 #}\n")
    (tmp_path / "page.html").write_text("{# zzz.py:1 #}\n")

    findings = _template_findings(_env_tmp(tmp_path), tmp_path)
    assert findings == [_Finding("page.html", 1, "jinja comment", "zzz.py:1")]


def test_ob13_undecodable_template_fails_loudly_and_is_named(tmp_path: Path) -> None:
    (tmp_path / "bad.html").write_bytes(b"\x80")
    (tmp_path / "page.html").write_text("<p>clean</p>\n")

    with pytest.raises(UnicodeDecodeError) as excinfo:
        _template_findings(_env_tmp(tmp_path), tmp_path)
    assert "while reading template bad.html" in excinfo.value.__notes__
