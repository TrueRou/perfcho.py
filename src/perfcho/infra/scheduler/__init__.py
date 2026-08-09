"""Application scheduler configuration."""

from datetime import timedelta

from apscheduler import AsyncScheduler, CoalescePolicy, ConflictPolicy
from apscheduler.triggers.cron import CronTrigger

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.scheduler.rank_snapshot import RankSnapshotTask

_RANK_SNAPSHOT_SCHEDULE_ID = "rank_snapshot"
_RANK_SNAPSHOT_MISFIRE_GRACE_SECONDS = 3600


async def configure_scheduler(
    scheduler: AsyncScheduler,
    session_factory: DbSessionFactory,
    *,
    rank_snapshot_cron: str,
) -> None:
    """Register all application schedules on one caller-owned scheduler."""
    rank_snapshot = RankSnapshotTask(session_factory)
    await scheduler.configure_task(
        _RANK_SNAPSHOT_SCHEDULE_ID,
        func=rank_snapshot.run,
        max_running_jobs=1,
    )
    await scheduler.add_schedule(
        _RANK_SNAPSHOT_SCHEDULE_ID,
        CronTrigger.from_crontab(rank_snapshot_cron),
        id=_RANK_SNAPSHOT_SCHEDULE_ID,
        coalesce=CoalescePolicy.latest,
        conflict_policy=ConflictPolicy.replace,
        misfire_grace_time=timedelta(seconds=_RANK_SNAPSHOT_MISFIRE_GRACE_SECONDS),
    )


async def start_scheduler(
    session_factory: DbSessionFactory,
    *,
    rank_snapshot_cron: str,
) -> AsyncScheduler:
    """Create, configure, and start one process-owned scheduler."""
    scheduler = AsyncScheduler()
    await scheduler.__aenter__()
    try:
        await configure_scheduler(
            scheduler,
            session_factory,
            rank_snapshot_cron=rank_snapshot_cron,
        )
        await scheduler.start_in_background()
    except BaseException:
        await scheduler.__aexit__(None, None, None)
        raise
    return scheduler


async def stop_scheduler(scheduler: AsyncScheduler) -> None:
    """Stop and close one process-owned scheduler."""
    await scheduler.__aexit__(None, None, None)


__all__ = ["RankSnapshotTask", "configure_scheduler", "start_scheduler", "stop_scheduler"]
