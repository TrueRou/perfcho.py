"""Create the central FastAPI process role and its shared resources."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from time import monotonic_ns
from typing import TypedDict

from fastapi import FastAPI

from perfcho.api import router
from perfcho.api.signalr import register_signalr
from perfcho.api.tracing import TraceContextMiddleware
from perfcho.infra import logging
from perfcho.infra.compose import CoreServices, StableServices, compose_core_services, compose_stable_services


class AppState(TypedDict):
    """Shared state for the FastAPI application."""

    core_services: CoreServices
    stable_services: StableServices


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[AppState]:
    """Create process-owned resources and run application schedules."""
    del app
    started_ns = monotonic_ns()
    core_services: CoreServices | None = None
    stable_services: StableServices | None = None
    logging.log_event("INFO", "runtime.api.starting")

    try:
        core_services = await compose_core_services()
        stable_services = await compose_stable_services(core_services)
        yield {
            "core_services": core_services,
            "stable_services": stable_services,
        }
        logging.log_event("INFO", "runtime.api.ready", duration_ms=logging.duration_ms(started_ns))
    except Exception as error:
        logging.log_event(
            "ERROR",
            "runtime.api.startup_failed",
            exception=error,
            error_type=type(error).__name__,
            duration_ms=logging.duration_ms(started_ns),
        )
        raise
    finally:
        if core_services:
            await core_services.aclose()
        logging.log_event("INFO", "runtime.api.stopped", duration_ms=logging.duration_ms(started_ns))


def init_routes(asgi_app: FastAPI) -> None:
    """Attach all HTTP and protocol adapters to the application."""
    asgi_app.include_router(router)
    register_signalr(asgi_app)


def create_app() -> FastAPI:
    """Construct an application without starting external resources."""
    logging.init_logger("api")
    asgi_app = FastAPI(
        title="perfcho.py",
        version="0.1.0",
        lifespan=lifespan,
    )
    asgi_app.add_middleware(TraceContextMiddleware)

    @asgi_app.get("/")
    async def root() -> dict:
        return {"message": "Welcome to perfcho.py"}

    init_routes(asgi_app)
    return asgi_app


asgi_app = create_app()
