"""Translate framework and unexpected exceptions into API responses."""

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from perfcho.api.v1.response import ResponseHandler
from perfcho.infra.logging import source

unexpected_error_response = ResponseHandler.error("An unexpected error occurred on the server.").model_dump()


class ExceptionHandlerMiddleware(BaseHTTPMiddleware):
    """Catch unhandled endpoint errors and hide internal details."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Execute one request and map unexpected failures to HTTP 500."""
        try:
            response = await call_next(request)
            return response
        except Exception as exc:
            logger.opt(exception=exc).patch(source()).exception(str(exc))
            return JSONResponse(status_code=500, content=unexpected_error_response)


def add_middleware(asgi_app: FastAPI) -> None:
    """Attach the unexpected-exception middleware to an application."""
    asgi_app.add_middleware(ExceptionHandlerMiddleware)


def add_exception_handler(asgi_app: FastAPI) -> None:
    """Register validation and explicit HTTP exception handlers."""

    @asgi_app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = exc.errors()
        if errors:
            error = errors[0]
            field = " -> ".join(str(loc) for loc in error.get("loc", []))
            msg = f"Request validation failed: {field} - {error.get('msg', 'unknown error')}"
        else:
            msg = "Request validation failed"

        return JSONResponse(status_code=422, content=ResponseHandler.error(msg, 422).model_dump())

    @asgi_app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        logger.patch(source()).warning("{} {} ({})", request.method, request.url, str(exc.detail))
        return JSONResponse(
            status_code=exc.status_code,
            content=ResponseHandler.error(exc.detail, exc.status_code).model_dump(),
        )
