"""Run benchmarks with ``python -m tools.benchmark``."""

import argparse
import asyncio
import os
from pathlib import Path

from .metrics import summarize
from .models import BenchmarkConfig
from .report import write_report
from .runner import run_benchmark
from .scoring import ScoringDatabaseScenario, ScoringRedisScenario, scoring_environment


async def main() -> None:
    """Parse options and run the selected scoring benchmark."""
    parser = argparse.ArgumentParser(description="Run isolated perfcho benchmarks")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", "postgresql+asyncpg://perfcho:perfcho@127.0.0.1:55432/perfcho"))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_CACHE_URL"))
    parser.add_argument("--players", type=int, default=50_000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--output", type=Path, default=Path("artifacts/benchmark/scoring"))
    args = parser.parse_args()
    config = BenchmarkConfig(args.warmup, args.duration, args.concurrency)
    rows = []
    async with scoring_environment(args.database_url, args.players) as (_, session_factory, schema):
        for name in ("postgres-personal-rank", "postgres-top-n"):
            scenario = ScoringDatabaseScenario(session_factory, name, args.players, schema)
            rows.append(summarize(await run_benchmark(scenario, config)))
        if args.redis_url:
            from redis.asyncio import Redis

            redis = Redis.from_url(args.redis_url, decode_responses=False)
            key = "perfbench:scoring:performance"
            await redis.delete(key)
            await redis.zadd(
                key,
                {str(account_id): account_id % 10_000 / 10 for account_id in range(1, args.players + 1)},
            )
            try:
                for name in ("redis-personal-rank", "redis-top-n"):
                    scenario = ScoringRedisScenario(redis, name, key, args.players)
                    rows.append(summarize(await run_benchmark(scenario, config)))
            finally:
                await redis.delete(key)
                await redis.aclose()
    write_report(args.output, f"Scoring leaderboard benchmark ({args.players:,} players)", rows)
    print(f"wrote {args.output}.md and {args.output}.json")


if __name__ == "__main__":
    asyncio.run(main())
