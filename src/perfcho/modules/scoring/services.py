"""Accept canonical Stable and Lazer scores in one explicit transaction."""

import hashlib
import json
import uuid
from collections.abc import Callable
from datetime import timedelta

from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import Clock, IdGenerator, ObjectStorage
from perfcho.modules.scoring.errors import (
    BeatmapRevisionNotFound,
    PerformanceCalculationError,
    ReplayNotFound,
    ScoreboardUnavailable,
    ScoreRejected,
)
from perfcho.modules.scoring.models import (
    AcceptedScoreResult,
    AcceptScore,
    AccountStatsView,
    BeatmapGradeView,
    ClientFamily,
    LeaderboardPage,
    PerformanceResult,
    PlayAttemptRecord,
    ReplayReference,
    Ruleset,
    ScoreAcceptanceRecord,
    ScoreboardVariant,
    ScorePerformanceView,
    thaw_json_mapping,
)
from perfcho.modules.scoring.mods import normalize_mods
from perfcho.modules.scoring.ports import (
    AccountSubmissionValidatorFactory,
    MultiplayerSubmissionValidatorFactory,
    PerformanceCalculationRepositoryFactory,
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
        clock: Clock,
        id_generator: IdGenerator,
        *,
        receipt_ttl: timedelta = _RECEIPT_TTL,
    ) -> None:
        """Bind transaction, validation, event, clock, and ID ports."""
        if receipt_ttl <= timedelta(0):
            raise ValueError("receipt_ttl must be positive")
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._account_validator_factory = account_validator_factory
        self._multiplayer_validator_factory = multiplayer_validator_factory
        self._clock = clock
        self._id_generator = id_generator
        self._receipt_ttl = receipt_ttl

    async def accept(self, command: AcceptScore) -> AcceptedScoreResult:
        """Accept score facts, replay evidence, calculation jobs, and one durable event."""
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
        if command.attempt.ended_at > now + timedelta(minutes=5):
            raise ScoreRejected("score end time is unreasonably far in the future")

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
            validated = validate_score(
                command.ruleset,
                normalized_mods.mods,
                command.attempt,
                command.score,
                revision,
                command.variant,
            )

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
                    attempt=command.attempt,
                    at=now,
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
                    processed_at=now,
                )
            )
            performance_release_ids = await repository.schedule_performance_calculations(
                score_id=result.score_id,
                scoreboard_id=scoreboard.scoreboard_id,
                ruleset=scoreboard.ruleset,
                now=now,
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
                        "performance_release_ids": [str(release_id) for release_id in performance_release_ids],
                    },
                    consumers=(_RANKING_CONSUMER,),
                    partition_key=f"scoreboard:{scoreboard.scoreboard_id}",
                )
            )
            await repository.complete_acceptance(command.meta.idempotency_key, result)
            await uow.commit()
            return result


class PerformanceCalculationService:
    """Execute one leased calculation without holding a transaction over external I/O."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: PerformanceCalculationRepositoryFactory,
        outbox_writer_factory: ScoringOutboxWriterFactory,
        calculator: PerformanceCalculator,
        object_storage: ObjectStorage,
        clock: Clock,
        *,
        max_attempts: int,
        max_beatmap_bytes: int,
        max_retry_seconds: int,
    ) -> None:
        """Bind phased persistence, pure calculation, storage, and retry policy."""
        if min(max_attempts, max_beatmap_bytes, max_retry_seconds) < 1:
            raise ValueError("performance calculation limits must be positive")
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._calculator = calculator
        self._object_storage = object_storage
        self._clock = clock
        self._max_attempts = max_attempts
        self._max_beatmap_bytes = max_beatmap_bytes
        self._max_retry_seconds = max_retry_seconds

    async def execute(self, job_id: uuid.UUID, lease_token: uuid.UUID) -> None:
        """Load, calculate, and persist one fenced job through separate transactions."""
        started_at = self._clock.now()
        try:
            async with self._uow_factory() as uow:
                repository = self._repository_factory(uow.session)
                calculation = await repository.start(job_id, lease_token, now=started_at)
                await uow.commit()
        except Exception as error:
            retryable = not isinstance(error, PerformanceCalculationError) or error.retryable
            retry_at = started_at + timedelta(seconds=min(2, self._max_retry_seconds))
            async with self._uow_factory() as uow:
                repository = self._repository_factory(uow.session)
                await repository.fail(
                    job_id,
                    lease_token,
                    error=f"{type(error).__name__}: {error}"[:4000],
                    retry_at=retry_at,
                    dead=not retryable,
                    consume_attempt=True,
                    now=started_at,
                )
                await uow.commit()
            return
        if calculation is None:
            return

        try:
            beatmap_content = await self._read_beatmap(calculation.beatmap_storage_key, calculation.beatmap_sha256)
            result = await self._calculator.calculate(calculation, beatmap_content)
            output_digest = _performance_output_digest(result)
            async with self._uow_factory() as uow:
                completion = await self._repository_factory(uow.session).complete(
                    calculation,
                    lease_token,
                    result,
                    output_digest=output_digest,
                    now=self._clock.now(),
                )
                if completion is not None:
                    await self._outbox_writer_factory(uow.session).append(
                        PendingEvent(
                            aggregate_type="score",
                            aggregate_id=str(completion.score_id),
                            event_type="score.performance-calculated.v1",
                            schema_version=1,
                            payload={
                                "score_id": completion.score_id,
                                "scoreboard_id": completion.scoreboard_id,
                                "formula_id": str(completion.formula_id),
                                "formula_code": completion.formula_code,
                                "release_id": str(completion.release_id),
                                "pp": str(completion.pp),
                                "output_digest": completion.output_digest.hex(),
                            },
                            consumers=(_RANKING_CONSUMER,),
                            partition_key=f"scoreboard:{completion.scoreboard_id}",
                        )
                    )
                await uow.commit()
        except Exception as error:
            await self._record_failure(calculation.job_id, lease_token, calculation.attempt_count, error)

    async def _read_beatmap(self, storage_key: str, expected_sha256: bytes) -> bytes:
        digest = hashlib.sha256()
        content = bytearray()
        async with self._object_storage.open(storage_key) as stream:
            if stream.metadata.size_bytes > self._max_beatmap_bytes:
                raise PerformanceCalculationError("beatmap object exceeds the calculation limit", retryable=False)
            async for chunk in stream.iter_chunks():
                content.extend(chunk)
                digest.update(chunk)
                if len(content) > self._max_beatmap_bytes:
                    raise PerformanceCalculationError("beatmap object exceeds the calculation limit", retryable=False)
        if digest.digest() != expected_sha256:
            raise PerformanceCalculationError("beatmap object digest does not match its revision", retryable=False)
        return bytes(content)

    async def _record_failure(
        self,
        job_id: uuid.UUID,
        lease_token: uuid.UUID,
        attempt_count: int,
        error: Exception,
    ) -> None:
        now = self._clock.now()
        retryable = not isinstance(error, PerformanceCalculationError) or error.retryable
        dead = not retryable or attempt_count >= self._max_attempts
        delay = min(2 ** max(attempt_count, 1), self._max_retry_seconds)
        async with self._uow_factory() as uow:
            await self._repository_factory(uow.session).fail(
                job_id,
                lease_token,
                error=f"{type(error).__name__}: {error}"[:4000],
                retry_at=now + timedelta(seconds=delay),
                dead=dead,
                consume_attempt=False,
                now=now,
            )
            await uow.commit()


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


class PerformanceQueryService:
    """Read all Formula-owned PP results for one accepted score."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: ScoringRepositoryFactory,
    ) -> None:
        """Bind short-lived query transactions and scoring persistence."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory

    async def list_for_score(self, score_id: int) -> tuple[ScorePerformanceView, ...]:
        """Return all persisted releases grouped by Formula metadata."""
        _positive_identifier("score_id", score_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).get_score_performances(score_id)


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

    async def get_account_stats(
        self,
        account_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant = ScoreboardVariant.VANILLA,
    ) -> AccountStatsView:
        """Return current score totals while leaving Performance explicitly deferred."""
        _positive_identifier("account_id", account_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).get_account_stats(account_id, ruleset, variant)

    async def get_beatmap_grades(
        self,
        account_id: int,
        beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapGradeView, ...]:
        """Return real projected vanilla grades, leaving absent modes as absent."""
        _positive_identifier("account_id", account_id)
        if len(beatmap_ids) > 2048 or any(identifier < 1 for identifier in beatmap_ids):
            raise ValueError("beatmap grade selectors are invalid")
        if not beatmap_ids:
            return ()
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).get_beatmap_grades(account_id, beatmap_ids)


def _positive_identifier(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


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
