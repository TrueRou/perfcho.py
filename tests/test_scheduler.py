from unittest.mock import MagicMock

import pytest
from apscheduler import AsyncScheduler, CoalescePolicy
from apscheduler.triggers.cron import CronTrigger

from perfcho.infra.scheduler import configure_scheduler


@pytest.mark.asyncio
async def test_user_ranking_snapshot_schedule_is_coalesced_and_single_instance() -> None:
    async with AsyncScheduler() as scheduler:
        await configure_scheduler(
            scheduler,
            MagicMock(),
            user_ranking_snapshot_cron="0 4 * * *",
        )

        schedules = await scheduler.get_schedules()
        tasks = await scheduler.get_tasks()

    schedule = next(schedule for schedule in schedules if schedule.id == "user_ranking_snapshot")
    task = next(task for task in tasks if task.id == "user_ranking_snapshot")
    assert schedule.coalesce is CoalescePolicy.latest
    assert task.max_running_jobs == 1
    assert isinstance(schedule.trigger, CronTrigger)
