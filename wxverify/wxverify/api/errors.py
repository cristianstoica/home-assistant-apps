"""API exception handlers."""
# pyright: reportUnusedFunction=false

from __future__ import annotations

import math

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse

from wxverify.worker.control import JobDeferred

type _Json = None | bool | int | float | str | list[_Json] | dict[str, _Json]


def _finite_json(value: _Json) -> _Json:
    """Return ``value`` with each non-finite float spelled as a string.

    A bare ``NaN``, ``Infinity`` or ``-Infinity`` in a request body parses,
    fails validation, and is echoed back in the error's ``input`` (possibly
    nested inside it). ``JSONResponse`` renders with ``allow_nan=False``, so
    that echo would turn the 422 into a 500. Such values become the strings
    ``"NaN"``, ``"Infinity"`` and ``"-Infinity"``; everything else, finite
    floats included, comes back equal to the input.
    """
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, dict):
        return {key: _finite_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_finite_json(item) for item in value]
    return value


class ApiError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse({"error": exc.message}, status_code=exc.status_code)

    @app.exception_handler(JobDeferred)
    async def deferred_handler(request: Request, exc: JobDeferred) -> JSONResponse:
        return JSONResponse(
            {"error": "budget exhausted", "next_attempt_at": exc.next_attempt_at},
            status_code=503,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        detail = _finite_json(jsonable_encoder(exc.errors()))
        return JSONResponse(
            {"error": "validation failed", "detail": detail},
            status_code=422,
        )
