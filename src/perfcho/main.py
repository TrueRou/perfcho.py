"""Create the central FastAPI process role and its shared resources."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TypedDict

from fastapi import FastAPI
from loguru import logger
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from perfcho.api import router
from perfcho.api.v1 import response
from perfcho.api.v1.middleware import cors, error
from perfcho.infra import logging
from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.redis import engine as infra_redis
from perfcho.infra.settings import settings


class AppState(TypedDict):
    """Describe infrastructure resources available to each request."""

    db_engine: AsyncEngine
    redis_engine: Redis
    db_session_factory: DbSessionFactory


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[AppState]:
    """Create and dispose process-owned database and Redis clients."""
    logger.patch(logging.source("", "")).info(
        "Server starting, listening on: http://{}:{}", settings.app_host, settings.app_port
    )

    db_engine: AsyncEngine | None = None
    redis_engine: Redis | None = None

    try:
        db_engine = await infra_db.create_engine()
        redis_engine = await infra_redis.create_redis()
        yield {
            "db_engine": db_engine,
            "redis_engine": redis_engine,
            "db_session_factory": infra_db.create_session_factory(db_engine),
        }
    finally:
        await redis_engine.close() if redis_engine else None
        await db_engine.dispose() if db_engine else None


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
    logging.init_logger()
    openapi_url = "/openapi.json" if settings.app_debug else None
    asgi_app = FastAPI(
        title="perfcho.py",
        version="0.1.0",
        lifespan=lifespan,
        openapi_url=openapi_url,
    )

    @asgi_app.get("/")
    async def root() -> response.AppResponse[dict]:
        return response.ResponseHandler.success({"message": "Welcome to perfcho.py"})

    init_middlewares(asgi_app)
    init_routes(asgi_app)
    return asgi_app


asgi_app = create_app()
