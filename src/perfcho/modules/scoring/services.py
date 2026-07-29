"""Accept canonical Stable and Lazer scores in one explicit transaction."""

import uuid
from collections.abc import Callable
from datetime import timedelta

from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import Clock, IdGenerator
from perfcho.modules.scoring.errors import BeatmapRevisionNotFound, ReplayNotFound, ScoreboardUnavailable, ScoreRejected
from perfcho.modules.scoring.models import (
    AcceptedScoreResult,
    AcceptScore,
    ClientFamily,
    LeaderboardPage,
    PerformanceCalculationInput,
    PlayAttemptRecord,
    ReplayReference,
    Ruleset,
    ScoreAcceptanceRecord,
    ScoreboardVariant,
)
from perfcho.modules.scoring.mods import normalize_mods
from perfcho.modules.scoring.ports import (
    AccountSubmissionValidatorFactory,
    MultiplayerSubmissionValidatorFactory,
    PerformanceCalculator,
    ScoringOutboxWriterFactory,
    ScoringRepositoryFactory,
    ScoringUnitOfWork,
)
from perfcho.modules.scoring.validation import validate_score

_RECEIPT_TTL = timedelta(days=7)
_RANKING_CONSUMER = "ranking-projector.v1"


class ScoringService:
    """Validate and atomically persist one canonical score acceptance."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: ScoringRepositoryFactory,
        outbox_writer_factory: ScoringOutboxWriterFactory,
        account_validator_factory: AccountSubmissionValidatorFactory,
        multiplayer_validator_factory: MultiplayerSubmissionValidatorFactory,
        performance_calculator: PerformanceCalculator,
        clock: Clock,
        id_generator: IdGenerator,
        *,
        receipt_ttl: timedelta = _RECEIPT_TTL,
    ) -> None:
        """Bind transaction, validation, calculation, event, clock, and ID ports."""
        if receipt_ttl <= timedelta(0):
            raise ValueError("receipt_ttl must be positive")
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._account_validator_factory = account_validator_factory
        self._multiplayer_validator_factory = multiplayer_validator_factory
        self._performance_calculator = performance_calculator
        self._clock = clock
        self._id_generator = id_generator
        self._receipt_ttl = receipt_ttl

    async def accept(self, command: AcceptScore) -> AcceptedScoreResult:
        """Accept score facts, replay evidence, optional PP, and one durable event."""
        actor = command.meta.actor
        if actor is None:
            raise ScoreRejected("score submission requires an authenticated actor")
        try:
            protocol = ClientFamily(command.meta.client.family)
        except ValueError as error:
            raise ScoreRejected("unsupported score submission client family") from error
        if command.attestation.client_family is not protocol:
            raise ScoreRejected("attestation client family does not match the command client")
        if (
            command.meta.client.version is not None
            and command.attestation.client_version != command.meta.client.version
        ):
            raise ScoreRejected("attestation client version does not match the command client")
        normalized_mods = normalize_mods(command.ruleset, command.variant, command.mods)
        now = self._clock.now()

        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            claim = await repository.claim_acceptance(
                idempotency_key=command.meta.idempotency_key,
                request_digest=command.meta.request_digest,
                now=now,
                expires_at=now + self._receipt_ttl,
            )
            if claim.prior_result is not None:
                await uow.commit()
                return claim.prior_result

            account_context = await self._account_validator_factory(uow.session).validate(actor.account_id, at=now)
            revision = await repository.resolve_current_revision(command.beatmap)
            if revision is None:
                raise BeatmapRevisionNotFound("current beatmap revision was not found")
            if revision.ruleset is not command.ruleset and revision.ruleset.value != "osu":
                raise ScoreRejected("native beatmap ruleset cannot be converted to the submitted ruleset")
            scoreboard = await repository.get_scoreboard(command.ruleset, command.variant)
            if scoreboard is None:
                raise ScoreboardUnavailable("canonical scoreboard is not active")
            mod_set = await repository.get_or_create_mod_set(scoreboard.scoreboard_id, normalized_mods)
            validated = validate_score(command.ruleset, normalized_mods.mods, command.attempt, command.score, revision)

            attempt_claim = await repository.claim_attempt(
                PlayAttemptRecord(
                    attempt_id=self._id_generator.new(),
                    account_id=account_context.account_id,
                    beatmap_id=revision.beatmap_id,
                    beatmap_revision_id=revision.revision_id,
                    scoreboard_id=scoreboard.scoreboard_id,
                    mod_set_id=mod_set.mod_set_id,
                    protocol=protocol,
                    submission=command.attempt,
                    outcome=command.score.outcome,
                )
            )
            if attempt_claim.prior_result is not None:
                await repository.complete_acceptance(command.meta.idempotency_key, attempt_claim.prior_result)
                await uow.commit()
                return attempt_claim.prior_result

            multiplayer_validator = self._multiplayer_validator_factory(uow.session)
            if command.multiplayer is not None:
                await multiplayer_validator.validate(
                    command.multiplayer,
                    account_id=account_context.account_id,
                    revision=revision,
                    scoreboard=scoreboard,
                    mod_set=mod_set,
                    at=now,
                )

            performance = await self._performance_calculator.calculate(
                PerformanceCalculationInput(revision, scoreboard, normalized_mods, command.score)
            )
            result = await repository.insert_score(
                ScoreAcceptanceRecord(
                    attempt_id=attempt_claim.attempt_id,
                    account_id=account_context.account_id,
                    revision=revision,
                    scoreboard=scoreboard,
                    mod_set=mod_set,
                    attempt=command.attempt,
                    score=command.score,
                    replay=command.replay,
                    attestation=command.attestation,
                    validated=validated,
                    performance=performance,
                    processed_at=now,
                )
            )
            if command.multiplayer is not None:
                await multiplayer_validator.bind_score(
                    command.multiplayer,
                    play_attempt_id=result.attempt_id,
                    score_id=result.score_id,
                    at=now,
                )

            outbox_writer = self._outbox_writer_factory(uow.session)
            await outbox_writer.append(
                PendingEvent(
                    aggregate_type="score",
                    aggregate_id=str(result.score_id),
                    event_type="score.accepted.v1",
                    schema_version=1,
                    payload={
                        "score_id": result.score_id,
                        "attempt_id": str(result.attempt_id),
                        "account_id": account_context.account_id,
                        "country_code": account_context.country_code,
                        "beatmap_id": revision.beatmap_id,
                        "beatmap_revision_id": revision.revision_id,
                        "beatmap_status": revision.status,
                        "scoreboard_id": scoreboard.scoreboard_id,
                        "mod_set_id": mod_set.mod_set_id,
                        "outcome": result.outcome.value,
                        "grade": command.score.grade.value,
                        "total_hits": validated.total_hits,
                        "play_time_ms": int(
                            (command.attempt.ended_at - command.attempt.started_at).total_seconds() * 1000
                        ),
                        "ended_at": command.attempt.ended_at.isoformat(),
                        "request_id": str(command.meta.request_id),
                        "performance_release_id": str(performance.release_id) if performance is not None else None,
                    },
                    consumers=(_RANKING_CONSUMER,),
                    partition_key=f"scoreboard:{scoreboard.scoreboard_id}",
                )
            )
            await repository.complete_acceptance(command.meta.idempotency_key, result)
            await uow.commit()
            return result


class ReplayQueryService:
    """Resolve ready replay manifests through short-lived read sessions."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: ScoringRepositoryFactory,
    ) -> None:
        """Bind transaction and scoring repository factories."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory

    async def get(self, score_id: int) -> ReplayReference:
        """Return a ready replay reference or raise a protocol-neutral absence."""
        _positive_identifier("score_id", score_id)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).get_replay(score_id)
        if result is None:
            raise ReplayNotFound("score replay is unavailable")
        return result


class ReplayService:
    """Append idempotent replay view facts in explicit transactions."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: ScoringRepositoryFactory,
    ) -> None:
        """Bind transaction and scoring repository factories."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory

    async def record_view(
        self,
        *,
        request_id: uuid.UUID,
        replay: ReplayReference,
        viewer_account_id: int | None,
    ) -> None:
        """Record one view unless the owner downloaded their own replay."""
        if viewer_account_id is not None:
            _positive_identifier("viewer_account_id", viewer_account_id)
        if viewer_account_id == replay.owner_account_id:
            return
        async with self._uow_factory() as uow:
            await self._repository_factory(uow.session).record_replay_view(
                request_id=request_id,
                score_id=replay.score_id,
                score_owner_account_id=replay.owner_account_id,
                viewer_account_id=viewer_account_id,
            )
            await uow.commit()


class RankingQueryService:
    """Read bounded Stable leaderboard projections through canonical dimensions."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: ScoringRepositoryFactory,
    ) -> None:
        """Bind transaction and scoring repository factories."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory

    async def get_stable_leaderboard(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
        leaderboard_type: int,
        legacy_mod_bits: int,
        requester_account_id: int,
        friend_account_ids: tuple[int, ...] = (),
        limit: int = 50,
    ) -> LeaderboardPage:
        """Return a validated Stable local, top, mods, friends, or country page."""
        _positive_identifier("beatmap_id", beatmap_id)
        _positive_identifier("requester_account_id", requester_account_id)
        if leaderboard_type not in range(5):
            raise ValueError("leaderboard_type must be between zero and four")
        if legacy_mod_bits < 0 or not 1 <= limit <= 100:
            raise ValueError("leaderboard mods or limit is invalid")
        if any(identifier < 1 for identifier in friend_account_ids):
            raise ValueError("friend account IDs must be positive")
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).get_leaderboard(
                beatmap_id=beatmap_id,
                ruleset=ruleset,
                variant=variant,
                leaderboard_type=leaderboard_type,
                legacy_mod_bits=legacy_mod_bits,
                requester_account_id=requester_account_id,
                friend_account_ids=friend_account_ids,
                limit=limit,
            )


def _positive_identifier(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
