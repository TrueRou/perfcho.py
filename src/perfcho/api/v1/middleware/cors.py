from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from perfcho.infra.settings import settings


def add_middleware(asgi_app: FastAPI) -> None:
    asgi_app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
