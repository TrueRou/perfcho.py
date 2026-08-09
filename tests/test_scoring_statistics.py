import pytest
from sqlalchemy import func, select

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.models.scoring import RankingPolicy, RankSnapshot, UserRankedStat
from perfcho.infra.scheduler.rank_snapshot import RankSnapshotTask


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_daily_rank_snapshot_is_atomic_and_idempotent(postgres_database_url: str) -> None:
    del postgres_database_url
    engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(engine)
    try:
        async with session_factory.begin() as session:
            policy_id = await session.scalar(
                select(RankingPolicy.id).where(
                    RankingPolicy.scoreboard_id == 1,
                    RankingPolicy.is_default.is_(True),
                )
            )
            assert policy_id is not None
            session.add(
                UserRankedStat(
                    account_id=1,
                    policy_id=policy_id,
                    ranked_score=1_000_000,
                    performance=0,
                    accuracy=1,
                    grade_counts={"XH": 0, "X": 1, "SH": 0, "S": 0, "A": 0},
                )
            )

        task = RankSnapshotTask(session_factory)
        assert await task.run()
        assert not await task.run()

        async with session_factory() as session:
            assert await session.scalar(select(func.count()).select_from(RankSnapshot)) == 1
            snapshot = await session.scalar(select(RankSnapshot))
            assert snapshot is not None
            assert (snapshot.global_rank, snapshot.value) == (1, 1_000_000)
    finally:
        await engine.dispose()
