"""Scoring leaderboard benchmark adapter.

The adapter uses an isolated schema so a benchmark never writes application facts.
"""

import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


@dataclass(frozen=True, slots=True)
class ScoringBenchmarkConfig:
    """Define the generated leaderboard population."""

    player_count: int = 50_000
    policy_id: int = 1


class RedisRankingClient(Protocol):
    """Minimal Redis contract needed by the ZSET adapter."""

    async def zrevrank(self, key: str, member: str) -> int | None:
        """Return a member's reverse rank."""
        ...

    async def zrevrange(self, key: str, start: int, end: int, *, withscores: bool) -> object:
        """Return a reverse ordered range."""
        ...


class ScoringDatabaseScenario:
    """Execute the current PostgreSQL ranking queries against generated data."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        name: str,
        player_count: int,
        schema: str,
    ) -> None:
        """Bind one query workload to an isolated generated schema."""
        self.name = name
        self._session_factory = session_factory
        self._player_count = player_count
        self._schema = schema

    async def setup(self) -> None:
        """Keep the scenario lifecycle uniform; data is prepared by the environment."""
        return None

    async def run(self) -> object:
        """Execute either the current personal-rank or Top-N SQL shape."""
        async with self._session_factory() as session:
            if self.name == "postgres-personal-rank":
                value = Decimal(str(random.randint(0, 10_000)))
                return await session.scalar(
                    text(
                        f"SELECT count(*) FROM {self._schema}.user_ranked_stats "
                        "WHERE policy_id = 1 AND performance > :value"
                    ),
                    {"value": value},
                )
            return (
                await session.execute(
                    text(
                        f"SELECT account_id, performance FROM {self._schema}.user_ranked_stats "
                        "WHERE policy_id = 1 ORDER BY performance DESC, account_id ASC LIMIT 50"
                    )
                )
            ).all()

    async def teardown(self) -> None:
        """Keep the scenario lifecycle uniform; the environment owns cleanup."""
        return None


class ScoringRedisScenario:
    """Execute ZSET rank and top-N operations as a Redis comparison baseline."""

    def __init__(self, redis: RedisRankingClient, name: str, key: str, player_count: int) -> None:
        """Bind one ZSET workload to a benchmark-owned Redis key."""
        self.name = name
        self._redis = redis
        self._key = key
        self._player_count = player_count

    async def setup(self) -> None:
        """Keep the scenario lifecycle uniform; the caller seeds the ZSET."""
        return None

    async def run(self) -> object:
        """Execute ZREVRANK or ZREVRANGE against the generated ZSET."""
        account_id = random.randint(1, self._player_count)
        if self.name == "redis-personal-rank":
            return await self._redis.zrevrank(self._key, str(account_id))
        return await self._redis.zrevrange(self._key, 0, 49, withscores=True)

    async def teardown(self) -> None:
        """Keep the scenario lifecycle uniform; the caller owns key cleanup."""
        return None


@asynccontextmanager
async def scoring_environment(
    database_url: str,
    player_count: int,
) -> AsyncIterator[tuple[AsyncEngine, async_sessionmaker[AsyncSession], str]]:
    """Create and seed the benchmark-only PostgreSQL schema."""
    schema = f"perfbench_scoring_{uuid4().hex[:12]}"
    engine = create_async_engine(database_url, pool_size=20, max_overflow=30, pool_pre_ping=True)
    async with engine.begin() as connection:
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        await connection.execute(
            text(
                f"CREATE TABLE {schema}.user_ranked_stats ("
                "account_id integer NOT NULL, policy_id integer NOT NULL, "
                "ranked_score bigint NOT NULL, performance numeric(14,5) NOT NULL, "
                "PRIMARY KEY (account_id, policy_id))"
            )
        )
        await connection.execute(
            text(
                f"CREATE INDEX user_ranked_stats_performance_idx ON {schema}.user_ranked_stats "
                "(policy_id, performance, account_id)"
            )
        )
        await connection.execute(
            text(
                f"CREATE INDEX user_ranked_stats_score_idx ON {schema}.user_ranked_stats "
                "(policy_id, ranked_score, account_id)"
            )
        )
        await connection.execute(
            text(
                f"INSERT INTO {schema}.user_ranked_stats "
                "SELECT n, 1, n * 100000, (n % 10000)::numeric / 10 "
                "FROM generate_series(1, :count) AS n"
            ),
            {"count": player_count},
        )
        await connection.execute(text(f"ANALYZE {schema}.user_ranked_stats"))
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield engine, factory, schema
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await engine.dispose()
