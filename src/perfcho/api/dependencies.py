from collections.abc import AsyncIterator
from typing import cast

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.engine import DbSessionFactory, session_scope


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    session_factory = cast(DbSessionFactory, request.app.state.db_session_factory)
    async for session in session_scope(session_factory):
        yield session
