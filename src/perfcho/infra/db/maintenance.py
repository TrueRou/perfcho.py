"""Run resumable PostgreSQL-owned maintenance projections."""

from datetime import date

from sqlalchemy import Numeric, case, cast, func, literal, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.base import DbSessionFactory
from perfcho.infra.db.enums import OutboxDeliveryStatus
from perfcho.infra.db.models.core import Account
from perfcho.infra.db.models.events import OutboxDelivery
from perfcho.infra.db.models.scoring import RankingPolicy, RankSnapshot, UserRankedStat
from perfcho.infra.db.models.system import MaintenanceState

_RANK_SNAPSHOT_TASK = "ranking.daily-snapshot.v1"
_RANKING_CONSUMER = "ranking-projector.v1"
_RANK_SNAPSHOT_LOCK_ID = 0x72616E6B736E6170


class RankSnapshotMaintenance:
    """Materialize one complete daily rank snapshot after ranking catches up."""

    def __init__(self, session_factory: DbSessionFactory) -> None:
        """Bind maintenance work to short caller-independent transactions."""
        self._session_factory = session_factory

    async def run_due(self) -> bool:
        """Write today's snapshot once, returning whether work was committed."""
        async with self._session_factory.begin() as session:
            await session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": _RANK_SNAPSHOT_LOCK_ID},
            )
            today = await session.scalar(select(func.current_date()))
            if not isinstance(today, date):
                raise RuntimeError("PostgreSQL did not return a rank snapshot date")
            state = await session.get(MaintenanceState, _RANK_SNAPSHOT_TASK, with_for_update=True)
            if state is not None and state.state.get("snapshot_date") == today.isoformat():
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
            await _write_rank_snapshots(session, today)
            if state is None:
                session.add(MaintenanceState(task=_RANK_SNAPSHOT_TASK, state={"snapshot_date": today.isoformat()}))
            else:
                state.state = {"snapshot_date": today.isoformat()}
            return True


async def _write_rank_snapshots(session: AsyncSession, snapshot_date: date) -> None:
    ranking_value = case(
        (RankingPolicy.metric == "pp", UserRankedStat.performance),
        else_=cast(UserRankedStat.ranked_score, Numeric(20, 5)),
    ).label("value")
    active_stats = (
        select(
            UserRankedStat.policy_id,
            UserRankedStat.account_id,
            Account.country_code,
            ranking_value,
        )
        .join(RankingPolicy, RankingPolicy.id == UserRankedStat.policy_id)
        .join(Account, Account.id == UserRankedStat.account_id)
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
    statement = insert(RankSnapshot).from_select(
        ("policy_id", "snapshot_date", "account_id", "global_rank", "country_rank", "value"),
        ranked,
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=(RankSnapshot.policy_id, RankSnapshot.snapshot_date, RankSnapshot.account_id),
            set_={
                "global_rank": statement.excluded.global_rank,
                "country_rank": statement.excluded.country_rank,
                "value": cast(statement.excluded.value, Numeric(20, 5)),
            },
        )
    )
