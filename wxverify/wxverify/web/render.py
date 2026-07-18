"""Jinja rendering helpers with request-bound Ingress URLs and CSRF."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import jinja2
from fastapi import Request
from fastapi.responses import HTMLResponse

from wxverify import __version__, config
from wxverify.api.csrf import issue_csrf_pair, set_csrf_cookie
from wxverify.web.context import feed_label, variable_label_for

_WEB_DIR = Path(__file__).resolve().parent
_TEMPLATE_DIR = _WEB_DIR / "templates"
_STATIC_DIR = _WEB_DIR / "static"

env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(_TEMPLATE_DIR),
    autoescape=jinja2.select_autoescape(("html", "xml")),
)
# Callable helpers usable directly from any template. `variable_label_for` is
# kept DISTINCT from the `selected_variable_label` context string so a context
# key never shadows this global (Jinja builds its namespace as
# dict(globals, **render_vars)). `env.globals` is a plain dict at runtime; its
# stubbed value-union type rejects arbitrary callables, so cast to widen it.
_template_globals = cast("dict[str, object]", env.globals)
_template_globals["feed_label"] = feed_label
_template_globals["variable_label_for"] = variable_label_for
# Static assets are mounted at /static/<version>/ so every release produces
# new asset URLs (the HA frontend service worker caches /static/ paths
# cache-first and ignores query strings — only a path change busts it).
# Templates compose url('/static/' ~ static_version ~ '/<asset>').
_template_globals["static_version"] = __version__


def static_dir() -> Path:
    return _STATIC_DIR


def ingress_url(request: Request, path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    normalized_root_path = str(request.scope.get("root_path", "") or "").rstrip("/")
    return f"{normalized_root_path}{path}"


def active_page(request: Request) -> str:
    """Return the route path with the Ingress ``root_path`` prefix removed.

    Under HA Ingress, ``IngressPathMiddleware`` prepends the root path to
    ``scope["path"]``, so ``request.url.path`` arrives prefixed
    (e.g. ``/api/hassio_ingress/<token>/dashboard``). Stripping the root path
    yields the bare route (``/dashboard``) the nav compares against — an exact
    match that works standalone and under Ingress alike.
    """
    path = request.url.path
    root_path = str(request.scope.get("root_path", "") or "").rstrip("/")
    if root_path and path.startswith(root_path):
        path = path[len(root_path) :] or "/"
    return path


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
        active_page=active_page(request),
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
            active_page=active_page(request),
            **ctx,
        )
    )
