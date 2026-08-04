"""Create the central FastAPI process role and its shared resources."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from time import monotonic_ns
from typing import TypedDict

from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from perfcho.api import router
from perfcho.api.v1 import response
from perfcho.api.v1.middleware import cors, error
from perfcho.infra import logging
from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.glue.content import ContentRuntime, create_content_runtime
from perfcho.infra.glue.stable import StableServices, compose_stable_services
from perfcho.infra.redis import engine as infra_redis


class AppState(TypedDict):
    """Describe infrastructure resources available to each request."""

    db_engine: AsyncEngine
    redis_engine: Redis
    db_session_factory: DbSessionFactory
    content_runtime: ContentRuntime
    stable_services: StableServices


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[AppState]:
    """Create and dispose process-owned database and Redis clients."""
    started_ns = monotonic_ns()
    logging.log_event("INFO", "runtime.api.starting")
    db_engine: AsyncEngine | None = None
    redis_engine: Redis | None = None
    content_runtime: ContentRuntime | None = None
    ready = False

    try:
        db_engine = await infra_db.create_engine()
        redis_engine = await infra_redis.create_redis()
        session_factory = infra_db.create_session_factory(db_engine)
        content_runtime = create_content_runtime(session_factory)
        stable_services = compose_stable_services(
            session_factory,
            redis_engine,
            content_runtime=content_runtime,
        )
        logging.log_event("INFO", "runtime.api.ready", duration_ms=logging.duration_ms(started_ns))
        ready = True
        yield {
            "db_engine": db_engine,
            "redis_engine": redis_engine,
            "db_session_factory": session_factory,
            "content_runtime": content_runtime,
            "stable_services": stable_services,
        }
    except Exception as error:
        logging.log_event(
            "ERROR",
            "runtime.api.failed" if ready else "runtime.api.startup_failed",
            exception=error,
            error_type=type(error).__name__,
            duration_ms=logging.duration_ms(started_ns),
        )
        raise
    finally:
        logging.log_event("INFO", "runtime.api.stopping")
        if content_runtime is not None:
            try:
                await content_runtime.aclose()
            except Exception as error:
                logging.log_event(
                    "ERROR",
                    "runtime.api.resource_close_failed",
                    exception=error,
                    resource="content_upstream",
                    error_type=type(error).__name__,
                )
        if redis_engine is not None:
            try:
                await redis_engine.close()
            except Exception as error:
                logging.log_event(
                    "ERROR",
                    "runtime.api.resource_close_failed",
                    exception=error,
                    resource="redis",
                    error_type=type(error).__name__,
                )
        if db_engine is not None:
            try:
                await db_engine.dispose()
            except Exception as error:
                logging.log_event(
                    "ERROR",
                    "runtime.api.resource_close_failed",
                    exception=error,
                    resource="postgres",
                    error_type=type(error).__name__,
                )
        logging.log_event("INFO", "runtime.api.stopped", duration_ms=logging.duration_ms(started_ns))


def init_middlewares(asgi_app: FastAPI) -> None:
    """Attach cross-cutting middleware and exception translation."""
    cors.add_middleware(asgi_app)
    error.add_middleware(asgi_app)
    error.add_exception_handler(asgi_app)


def init_routes(asgi_app: FastAPI) -> None:
    """Attach all HTTP and protocol adapters to the application."""
    asgi_app.include_router(router)


def create_app() -> FastAPI:
    """Construct an application without starting external resources."""
    logging.init_logger("api")
    asgi_app = FastAPI(
        title="perfcho.py",
        version="0.1.0",
        lifespan=lifespan,
    )

    @asgi_app.get("/")
    async def root() -> response.AppResponse[dict]:
        return response.ResponseHandler.success({"message": "Welcome to perfcho.py"})

    init_middlewares(asgi_app)
    init_routes(asgi_app)
    return asgi_app


asgi_app = create_app()
