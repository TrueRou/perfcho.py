"""Project accepted scores into versioned Performance results."""

import hashlib
import json

from sqlalchemy.ext.asyncio import AsyncSession

from perfcho.infra.db.models.events import OutboxEvent
from perfcho.infra.db.projectors.common import advance_checkpoint, payload_integer, require_event_context
from perfcho.infra.db.repositories.outbox import SqlAlchemyOutboxWriter
from perfcho.infra.db.repositories.performance.projection import SqlAlchemyPerformanceProjectionRepository
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import ObjectUrlProvider
from perfcho.modules.performance.models import PerformanceResult, thaw_json_mapping
from perfcho.modules.performance.ports import PerformanceCalculator

CONSUMER_NAME = "performance-projector.v1"
EVENT_TYPES = frozenset({"score.accepted.v1"})
_RANKING_CONSUMER = "ranking-projector.v1"


class PerformanceProjector:
    """Calculate every matching release as a normal outbox projection."""

    def __init__(
        self,
        calculator: PerformanceCalculator,
        object_url_provider: ObjectUrlProvider,
        *,
        beatmap_url_expiry_seconds: int,
    ) -> None:
        """Bind external calculation dependencies and URL lifetime."""
        if beatmap_url_expiry_seconds < 1:
            raise ValueError("performance Beatmap URL expiry must be positive")
        self._calculator = calculator
        self._object_url_provider = object_url_provider
        self._beatmap_url_expiry_seconds = beatmap_url_expiry_seconds

    async def __call__(self, session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
        """Calculate and persist all Performance releases for one accepted score."""
        score_id = payload_integer(event.payload, "score_id")
        account_id = payload_integer(event.payload, "account_id")
        scoreboard_id = payload_integer(event.payload, "scoreboard_id")
        require_event_context(
            event,
            partition_key,
            aggregate_type="score",
            aggregate_id=str(score_id),
            expected_partition_key=f"account:{account_id}:scoreboard:{scoreboard_id}",
        )
        repository = SqlAlchemyPerformanceProjectionRepository(session)
        writer = SqlAlchemyOutboxWriter(session)
        for calculation in await repository.materialize(score_id):
            beatmap_url = await self._object_url_provider.presign_read(
                calculation.beatmap_storage_key,
                expires_in_seconds=self._beatmap_url_expiry_seconds,
            )
            result = await self._calculator.calculate(calculation, beatmap_url=beatmap_url)
            completion = await repository.complete(
                calculation,
                result,
                output_digest=_performance_output_digest(result),
            )
            await writer.append(
                PendingEvent(
                    aggregate_type="score",
                    aggregate_id=str(completion.score_id),
                    event_type="score.performance-calculated.v1",
                    schema_version=1,
                    payload={
                        "score_id": completion.score_id,
                        "account_id": completion.account_id,
                        "scoreboard_id": completion.scoreboard_id,
                        "formula_id": str(completion.formula_id),
                        "formula_code": completion.formula_code,
                        "release_id": str(completion.release_id),
                        "pp": str(completion.pp),
                        "output_digest": completion.output_digest.hex(),
                    },
                    consumers=(_RANKING_CONSUMER,),
                    partition_key=partition_key,
                )
            )
        await advance_checkpoint(session, event, projector=CONSUMER_NAME, partition_key=partition_key)


async def unconfigured_projector(session: AsyncSession, event: OutboxEvent, partition_key: str) -> None:
    """Reject execution when a runtime catalog omitted Performance dependencies."""
    del session, event, partition_key
    raise RuntimeError("performance projector is not configured")


def _performance_output_digest(result: PerformanceResult) -> bytes:
    payload = {
        "pp": str(result.pp),
        "difficulty": {
            "star_rating": str(result.difficulty.star_rating),
            "max_combo": result.difficulty.max_combo,
            "attributes": thaw_json_mapping(result.difficulty.attributes),
        },
        "breakdown": thaw_json_mapping(result.breakdown),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).digest()
