from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TypedDict

from fastapi import FastAPI
from loguru import logger

import perfcho.infra.redis as redis_infra
from perfcho.api import router
from perfcho.infra import engine, logging, response
from perfcho.infra.middleware import cors, error
from perfcho.infra.settings import settings


class AppState(TypedDict):
    db_connection: str
    debug_mode: bool


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    logger.patch(logging.source("", "")).info(
        "Server starting, listening on: http://{}:{}", settings.app_host, settings.app_port
    )
    db_engine = engine.create_database_engine()
    state_redis = redis_infra.create_state_redis()
    app.state.db_engine = db_engine
    app.state.db_session_factory = engine.create_session_factory(db_engine)
    app.state.redis = state_redis

    try:
        await engine.check_database(db_engine)
        await redis_infra.check_redis(state_redis)
        yield
    finally:
        await state_redis.aclose()
        await db_engine.dispose()


def init_middlewares(asgi_app: FastAPI) -> None:
    cors.add_middleware(asgi_app)
    error.add_middleware(asgi_app)
    error.add_exception_handler(asgi_app)


def init_routes(asgi_app: FastAPI) -> None:
    asgi_app.include_router(router)


def create_app() -> FastAPI:
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
