"""API exception handlers."""
# pyright: reportUnusedFunction=false

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse

from wxverify.worker.control import JobDeferred


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
        return JSONResponse(
            {"error": "validation failed", "detail": jsonable_encoder(exc.errors())},
            status_code=422,
        )
