"""Create daily user ranking snapshots from the current ranking projection."""

from datetime import date
from time import monotonic_ns

from sqlalchemy import Numeric, case, cast, func, literal, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra import logging
from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import OutboxDeliveryStatus
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.events import OutboxDelivery
from perfcho.infra.db.models.scoring import RankingPolicy, UserRanking, UserRankingSnapshot

_RANKING_CONSUMER = "ranking-projector.v1"
_USER_RANKING_SNAPSHOT_LOCK_ID = 0x72616E6B736E6170


class UserRankingSnapshotTask:
    """Materialize one complete user ranking snapshot after ranking catches up."""

    def __init__(self, session_factory: DbSessionFactory) -> None:
        """Bind the task to the process-owned database session factory."""
        self._session_factory = session_factory
        self._completed_date: date | None = None

    async def run(self) -> bool:
        """Write today's snapshot once, returning whether work was committed."""
        started_ns = monotonic_ns()
        try:
            completed = await self._run_once()
        except Exception as error:
            if logging.rate_limit("scheduler:user-ranking-snapshot:failed"):
                logging.log_event(
                    "ERROR",
                    "scheduler.user_ranking_snapshot.failed",
                    exception=error,
                    error_type=type(error).__name__,
                    duration_ms=logging.duration_ms(started_ns),
                )
            raise
        if completed:
            logging.log_event(
                "INFO",
                "scheduler.user_ranking_snapshot.completed",
                duration_ms=logging.duration_ms(started_ns),
            )
        return completed

    async def _run_once(self) -> bool:
        async with self._session_factory.begin() as session:
            acquired = await session.scalar(
                text("SELECT pg_try_advisory_xact_lock(:lock_id)"),
                {"lock_id": _USER_RANKING_SNAPSHOT_LOCK_ID},
            )
            if not acquired:
                return False
            today = await session.scalar(select(func.current_date()))
            if not isinstance(today, date):
                raise RuntimeError("PostgreSQL did not return a user ranking snapshot date")
            if self._completed_date == today:
                return False
            existing = await session.scalar(
                select(UserRankingSnapshot.policy_id)
                .where(UserRankingSnapshot.snapshot_date == today)
                .limit(1)
            )
            if existing is not None:
                self._completed_date = today
                return False
            unfinished = await session.scalar(
                select(func.count())
                .select_from(OutboxDelivery)
                .where(
                    OutboxDelivery.consumer == _RANKING_CONSUMER,
                    OutboxDelivery.status != OutboxDeliveryStatus.SUCCEEDED,
                )
            )
            if unfinished:
                return False
            await _write_user_ranking_snapshots(session, today)
        self._completed_date = today
        return True


async def _write_user_ranking_snapshots(session: AsyncSession, snapshot_date: date) -> None:
    metric = RankingPolicy.configuration["metric"].as_string()
    ranking_value = case(
        (metric == "pp", UserRanking.performance),
        else_=cast(UserRanking.ranked_score, Numeric(20, 5)),
    ).label("value")
    active_stats = (
        select(
            UserRanking.policy_id,
            UserRanking.account_id,
            Account.country_code,
            ranking_value,
        )
        .join(RankingPolicy, RankingPolicy.id == UserRanking.policy_id)
        .join(Account, Account.id == UserRanking.account_id)
        .where(RankingPolicy.active.is_(True), ranking_value > 0)
        .subquery()
    )
    ranked = select(
        active_stats.c.policy_id,
        literal(snapshot_date).label("snapshot_date"),
        active_stats.c.account_id,
        func.rank()
        .over(partition_by=active_stats.c.policy_id, order_by=active_stats.c.value.desc())
        .label("global_rank"),
        case(
            (
                active_stats.c.country_code.is_not(None),
                func.rank().over(
                    partition_by=(active_stats.c.policy_id, active_stats.c.country_code),
                    order_by=active_stats.c.value.desc(),
                ),
            ),
            else_=None,
        ).label("country_rank"),
        cast(active_stats.c.value, Numeric(20, 5)).label("value"),
    )
    statement = insert(UserRankingSnapshot).from_select(
        ("policy_id", "snapshot_date", "account_id", "global_rank", "country_rank", "value"),
        ranked,
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(
                UserRankingSnapshot.policy_id,
                UserRankingSnapshot.snapshot_date,
                UserRankingSnapshot.account_id,
            ),
            set_={
                "global_rank": statement.excluded.global_rank,
                "country_rank": statement.excluded.country_rank,
                "value": cast(statement.excluded.value, Numeric(20, 5)),
            },
        )
    )
