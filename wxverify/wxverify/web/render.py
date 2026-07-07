"""Jinja rendering helpers with request-bound Ingress URLs and CSRF."""

from __future__ import annotations

from pathlib import Path

import jinja2
from fastapi import Request
from fastapi.responses import HTMLResponse

from wxverify import config
from wxverify.api.csrf import issue_csrf_pair, set_csrf_cookie

_WEB_DIR = Path(__file__).resolve().parent
_TEMPLATE_DIR = _WEB_DIR / "templates"
_STATIC_DIR = _WEB_DIR / "static"

env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(_TEMPLATE_DIR),
    autoescape=jinja2.select_autoescape(("html", "xml")),
)


def static_dir() -> Path:
    return _STATIC_DIR


def ingress_url(request: Request, path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    normalized_root_path = str(request.scope.get("root_path", "") or "").rstrip("/")
    return f"{normalized_root_path}{path}"


def render(request: Request, template: str, **ctx: object) -> HTMLResponse:
    pair = issue_csrf_pair()
    tmpl = env.get_template(template)

    def url(path: object) -> str:
        return ingress_url(request, str(path))

    html = tmpl.render(
        app_title=config.APP_TITLE,
        url=url,
        csrf_token=pair.token,
        request_path=request.url.path,
        **ctx,
    )
    response = HTMLResponse(html)
    set_csrf_cookie(response, pair, str(request.scope.get("root_path", "") or ""))
    return response


def render_fragment(request: Request, template: str, **ctx: object) -> HTMLResponse:
    token = request.headers.get("x-csrf-token", "")
    tmpl = env.get_template(template)

    def url(path: object) -> str:
        return ingress_url(request, str(path))

    return HTMLResponse(
        tmpl.render(
            app_title=config.APP_TITLE,
            url=url,
            csrf_token=token,
            request_path=request.url.path,
            **ctx,
        )
    )
