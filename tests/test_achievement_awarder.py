import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest

from perfcho.modules.common.models import PendingEvent
from perfcho.modules.social.achievements import default_achievement_evaluator_registry
from perfcho.modules.social.models import (
    AchievementDefinitionRecord,
    AchievementEvaluationDefinition,
    AchievementUnlock,
    AchievementUnlockResult,
    ScoreAchievementContext,
)
from perfcho.modules.social.ports import SocialRepository
from perfcho.modules.social.services import TransactionAchievementAwarder

NOW = datetime(2026, 8, 3, 12, 0, tzinfo=UTC)


class FakeRepository:
    def __init__(self, definition: AchievementEvaluationDefinition | None) -> None:
        self.definition = definition
        self.unlocked: set[int] = set()

    async def list_score_achievement_definitions(
        self,
        *,
        account_id: int,
        ruleset: str,
    ) -> tuple[AchievementEvaluationDefinition, ...]:
        del account_id
        if self.definition is None:
            return ()
        if self.definition.achievement_id in self.unlocked or self.definition.ruleset not in {None, ruleset}:
            return ()
        return (self.definition,)

    async def unlock_achievement(
        self,
        *,
        account_id: int,
        definition: AchievementDefinitionRecord,
        score_id: int | None,
        source_event_id: uuid.UUID | None,
        snapshot: dict[str, object],
        now: datetime,
    ) -> AchievementUnlockResult:
        del source_event_id
        unlock = AchievementUnlock(
            account_id=account_id,
            achievement_id=definition.achievement_id,
            definition_version=definition.evaluator_version,
            score_id=score_id,
            source_event_id=None,
            snapshot=snapshot,
            created_at=now,
        )
        created = definition.achievement_id not in self.unlocked
        self.unlocked.add(definition.achievement_id)
        return AchievementUnlockResult(unlock, created)


class FakeOutbox:
    def __init__(self) -> None:
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.events.append(event)
        return uuid.uuid7()


def context(total_score: int = 1_000_000) -> ScoreAchievementContext:
    return ScoreAchievementContext(
        account_id=3,
        score_id=40,
        beatmap_id=10,
        beatmap_revision_id=20,
        ruleset="osu",
        variant="vanilla",
        beatmap_status="ranked",
        outcome="passed",
        grade="X",
        total_score=total_score,
        classic_score=total_score,
        accuracy=Decimal("1"),
        max_combo=10,
        perfect=True,
        total_hits=10,
        mods=(),
    )


@pytest.mark.asyncio
async def test_no_achievement_definitions_returns_no_unlocks() -> None:
    repository = FakeRepository(None)
    outbox = FakeOutbox()
    awarder = TransactionAchievementAwarder(
        cast(SocialRepository, repository),
        outbox,
        default_achievement_evaluator_registry(),
    )

    assert await awarder.award_for_score(context(), at=NOW) == ()
    assert outbox.events == []


@pytest.mark.asyncio
async def test_unknown_achievement_evaluator_is_safe_and_does_not_unlock() -> None:
    repository = FakeRepository(
        AchievementEvaluationDefinition(1, "unknown", "Unknown", "", "not_registered", 1, {}, None)
    )
    outbox = FakeOutbox()
    awarder = TransactionAchievementAwarder(
        cast(SocialRepository, repository),
        outbox,
        default_achievement_evaluator_registry(),
    )

    assert await awarder.award_for_score(context(), at=NOW) == ()
    assert repository.unlocked == set()
    assert outbox.events == []


@pytest.mark.asyncio
async def test_score_achievement_unlock_is_new_once_and_keeps_projector_event() -> None:
    repository = FakeRepository(
        AchievementEvaluationDefinition(
            1,
            "million-score",
            "Million Score",
            "Reach one million",
            "score_total_at_least",
            1,
            {"minimum": 1_000_000},
            "osu",
        )
    )
    outbox = FakeOutbox()
    awarder = TransactionAchievementAwarder(
        cast(SocialRepository, repository),
        outbox,
        default_achievement_evaluator_registry(),
    )

    first = await awarder.award_for_score(context(), at=NOW)
    second = await awarder.award_for_score(context(), at=NOW)

    assert [unlock.slug for unlock in first] == ["million-score"]
    assert second == ()
    assert len(outbox.events) == 1
    assert outbox.events[0].event_type == "social.achievement-unlocked.v1"
    assert outbox.events[0].consumers == ("achievement-projector.v1",)


@pytest.mark.asyncio
async def test_score_achievement_evaluator_requires_threshold() -> None:
    repository = FakeRepository(
        AchievementEvaluationDefinition(
            1,
            "million-score",
            "Million Score",
            "Reach one million",
            "score_total_at_least",
            1,
            {"minimum": 1_000_001},
            None,
        )
    )
    outbox = FakeOutbox()
    awarder = TransactionAchievementAwarder(
        cast(SocialRepository, repository),
        outbox,
        default_achievement_evaluator_registry(),
    )

    assert await awarder.award_for_score(context(), at=NOW) == ()
    assert outbox.events == []
