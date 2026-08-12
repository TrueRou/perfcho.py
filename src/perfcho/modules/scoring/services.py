"""Accept canonical score facts in one explicit transaction."""

import hashlib
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import cast

from perfcho.infra.cache import cached
from perfcho.infra.cache.backend import CacheBackend
from perfcho.infra.cache.values import decode_json, encode_json
from perfcho.infra.logging import duration_ms, log_event
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import Clock, IdGenerator, OutboxWriterFactory
from perfcho.modules.scoring.errors import (
    BeatmapRevisionNotFound,
    ReplayNotFound,
    ScoreboardUnavailable,
    ScoreRejected,
)
from perfcho.modules.scoring.models import (
    AcceptedScoreResult,
    AcceptScore,
    AccountStatsView,
    BeatmapGradeView,
    CanonicalMod,
    IssueSoloScoreToken,
    LeaderboardPage,
    LeaderboardScope,
    LeaderboardScopeKind,
    LeaderboardScoreView,
    PlayAttemptRecord,
    ReplayReference,
    Ruleset,
    ScoreAcceptanceRecord,
    ScoreboardVariant,
    ScoreDetailView,
    ScoreOutcome,
    SoloScoreToken,
)
from perfcho.modules.scoring.mods import normalize_mods
from perfcho.modules.scoring.ports import (
    AccountStatisticsRepositoryFactory,
    AccountSubmissionValidatorFactory,
    BeatmapScoresRepositoryFactory,
    MultiplayerSubmissionValidatorFactory,
    RankingRepositoryFactory,
    ReplayRepositoryFactory,
    ScoreQueryRepositoryFactory,
    ScoringAcceptanceRepositoryFactory,
    ScoringUnitOfWork,
)
from perfcho.modules.scoring.validation import validate_score
from perfcho.modules.social.models import ScoreAchievementContext
from perfcho.modules.social.ports import AchievementAwarderFactory

_RECEIPT_TTL = timedelta(days=7)
_RANKING_CONSUMER = "ranking-projector.v1"
_STATS_CONSUMER = "scoring-stats-projector.v1"
_MULTIPLAYER_RESULTS_CONSUMER = "multiplayer-results-projector.v1"
_PERFORMANCE_CONSUMER = "performance-projector.v1"


class ScoringService:
    """Validate and atomically persist one canonical score acceptance."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: ScoringAcceptanceRepositoryFactory,
        outbox_writer_factory: OutboxWriterFactory,
        account_validator_factory: AccountSubmissionValidatorFactory,
        multiplayer_validator_factory: MultiplayerSubmissionValidatorFactory,
        achievement_awarder_factory: AchievementAwarderFactory,
        clock: Clock,
        id_generator: IdGenerator,
        *,
        receipt_ttl: timedelta = _RECEIPT_TTL,
        solo_token_lifetime: timedelta = timedelta(hours=2),
    ) -> None:
        """Bind transaction, validation, event, clock, and ID ports."""
        if receipt_ttl <= timedelta(0):
            raise ValueError("receipt_ttl must be positive")
        if solo_token_lifetime <= timedelta(0):
            raise ValueError("solo_token_lifetime must be positive")
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._account_validator_factory = account_validator_factory
        self._multiplayer_validator_factory = multiplayer_validator_factory
        self._achievement_awarder_factory = achievement_awarder_factory
        self._clock = clock
        self._id_generator = id_generator
        self._receipt_ttl = receipt_ttl
        self._solo_token_lifetime = solo_token_lifetime

    async def issue_solo_token(self, command: IssueSoloScoreToken) -> SoloScoreToken:
        """Authorize one solo play after binding account, map revision, and ruleset."""
        actor = command.meta.actor
        if actor is None:
            raise ScoreRejected("solo score token requires an authenticated actor")
        now = self._clock.now()
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            await self._account_validator_factory(uow.session).validate(actor.account_id, at=now)
            revision = await repository.resolve_current_revision(command.beatmap)
            if revision is None:
                raise BeatmapRevisionNotFound("current beatmap revision was not found")
            if revision.ruleset is not command.ruleset and revision.ruleset is not Ruleset.OSU:
                raise ScoreRejected("native beatmap ruleset cannot be converted to the submitted ruleset")
            token = await repository.issue_solo_token(
                account_id=actor.account_id,
                revision=revision,
                ruleset=command.ruleset,
                started_at=now,
                expires_at=now + self._solo_token_lifetime,
            )
            await uow.commit()
            return token

    async def accept(self, command: AcceptScore) -> AcceptedScoreResult:
        """Accept score facts, optional replay evidence, follow-up work, and one durable event."""
        started_ns = time.monotonic_ns()
        actor = command.meta.actor
        if actor is None:
            raise ScoreRejected("score submission requires an authenticated actor")
        source = command.meta.client.family
        if command.attestation.source != source:
            raise ScoreRejected("attestation source does not match the command client")
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
                _log_acceptance(
                    "DEBUG",
                    "scoring.score.replayed",
                    claim.prior_result,
                    account_id=actor.account_id,
                    started_ns=started_ns,
                )
                return claim.prior_result

            account_context = await self._account_validator_factory(uow.session).validate(actor.account_id, at=now)
            solo_token: SoloScoreToken | None = None
            if command.solo_token_id is not None:
                solo_token = await repository.claim_solo_token(
                    command.solo_token_id,
                    account_id=actor.account_id,
                    beatmap_id=command.beatmap.beatmap_id or 0,
                    ruleset=command.ruleset,
                    at=now,
                )
                if solo_token.score_id is not None:
                    prior = await repository.accepted_result_for_score(solo_token.score_id)
                    if prior is None:
                        raise ScoreRejected("solo score token references an unavailable score")
                    await repository.complete_acceptance(command.meta.idempotency_key, prior)
                    await uow.commit()
                    return prior
                command = replace(
                    command,
                    attempt=replace(command.attempt, started_at=solo_token.started_at),
                )
            revision = await repository.resolve_current_revision(command.beatmap)
            if revision is None:
                raise BeatmapRevisionNotFound("current beatmap revision was not found")
            if solo_token is not None and revision.revision_id != solo_token.beatmap_revision_id:
                raise ScoreRejected("solo score token beatmap revision is no longer current")
            if solo_token is not None:
                judged_hits = _judged_hits(command.ruleset, command.score)
                progress = (
                    Decimal(1)
                    if command.score.outcome is ScoreOutcome.PASSED
                    else min(Decimal(judged_hits) / Decimal(max(revision.object_count, 1)), Decimal(1))
                )
                command = replace(command, attempt=replace(command.attempt, progress=progress))
            if revision.ruleset is not command.ruleset and revision.ruleset.value != "osu":
                raise ScoreRejected("native beatmap ruleset cannot be converted to the submitted ruleset")
            scoreboard = await repository.get_scoreboard(command.ruleset, command.variant)
            if scoreboard is None:
                raise ScoreboardUnavailable("canonical scoreboard is not active")
            mod_set = await repository.get_or_create_mod_set(scoreboard.scoreboard_id, normalized_mods)
            score = replace(
                command.score,
                perfect=(
                    command.score.outcome is ScoreOutcome.PASSED
                    and command.score.max_combo == revision.max_combo
                    and all(
                        statistic.actual == 0
                        for statistic in command.score.hits
                        if statistic.hit_result in {"miss", "small_tick_miss", "large_tick_miss"}
                    )
                ),
            )
            validated = validate_score(
                command.ruleset,
                normalized_mods.mods,
                command.attempt,
                score,
                revision,
                command.variant,
                uses_threshold_grading=command.uses_threshold_grading,
            )

            attempt_claim = await repository.claim_attempt(
                PlayAttemptRecord(
                    attempt_id=self._id_generator.new(),
                    account_id=account_context.account_id,
                    beatmap_id=revision.beatmap_id,
                    beatmap_revision_id=revision.revision_id,
                    scoreboard_id=scoreboard.scoreboard_id,
                    mod_set_id=mod_set.mod_set_id,
                    source=source,
                    submission=command.attempt,
                    outcome=score.outcome,
                )
            )
            if attempt_claim.prior_result is not None:
                await repository.complete_acceptance(command.meta.idempotency_key, attempt_claim.prior_result)
                await uow.commit()
                _log_acceptance(
                    "DEBUG",
                    "scoring.score.replayed",
                    attempt_claim.prior_result,
                    account_id=actor.account_id,
                    started_ns=started_ns,
                )
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
                    score=score,
                    replay=command.replay,
                    attestation=command.attestation,
                    validated=validated,
                    processed_at=now,
                )
            )
            new_unlocks = await self._achievement_awarder_factory(uow.session).award_for_score(
                ScoreAchievementContext(
                    account_id=account_context.account_id,
                    score_id=result.score_id,
                    beatmap_id=result.beatmap_id,
                    beatmap_revision_id=result.beatmap_revision_id,
                    ruleset=command.ruleset.value,
                    variant=command.variant.value,
                    beatmap_status=revision.status,
                    outcome=result.outcome.value,
                    grade=score.grade.value,
                    total_score=score.total_score,
                    classic_score=score.classic_score,
                    accuracy=validated.accuracy,
                    max_combo=score.max_combo,
                    perfect=score.perfect,
                    total_hits=validated.total_hits,
                    mods=tuple(mod.acronym for mod in normalized_mods.mods),
                ),
                at=now,
            )
            result = replace(result, new_achievement_unlocks=new_unlocks)
            if command.multiplayer is not None:
                await multiplayer_validator.bind_score(
                    command.multiplayer,
                    play_attempt_id=result.attempt_id,
                    score_id=result.score_id,
                    at=now,
                )
            if solo_token is not None:
                await repository.complete_solo_token(solo_token.token_id, result.score_id, at=now)

            outbox_writer = self._outbox_writer_factory(uow.session)
            consumers: tuple[str, ...] = (_RANKING_CONSUMER, _STATS_CONSUMER, _PERFORMANCE_CONSUMER)
            if command.multiplayer is not None:
                consumers += (_MULTIPLAYER_RESULTS_CONSUMER,)
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
                        "grade": score.grade.value,
                        "total_hits": validated.total_hits,
                        "play_time_ms": int(
                            (command.attempt.ended_at - command.attempt.started_at).total_seconds() * 1000
                        ),
                        "ended_at": command.attempt.ended_at.isoformat(),
                        "request_id": str(command.meta.request_id),
                    },
                    consumers=consumers,
                    partition_key=f"account:{account_context.account_id}:scoreboard:{scoreboard.scoreboard_id}",
                )
            )
            await repository.complete_acceptance(command.meta.idempotency_key, result)
            await uow.commit()
            _log_acceptance(
                "INFO",
                "scoring.score.accepted",
                result,
                account_id=account_context.account_id,
                started_ns=started_ns,
            )
            return result


def _judged_hits(ruleset: Ruleset, score: object) -> int:
    """Count ruleset object judgements for token progress."""
    from perfcho.modules.scoring.models import ScoreSubmission

    if not isinstance(score, ScoreSubmission):
        raise TypeError("score must be a ScoreSubmission")
    names = {
        Ruleset.OSU: {"great", "ok", "meh", "miss"},
        Ruleset.TAIKO: {"great", "ok", "miss"},
        Ruleset.FRUITS: {
            "great",
            "large_tick_hit",
            "large_tick_miss",
            "small_tick_hit",
            "small_tick_miss",
            "miss",
        },
        Ruleset.MANIA: {"perfect", "great", "good", "ok", "meh", "miss"},
    }[ruleset]
    return sum(statistic.actual for statistic in score.hits if statistic.hit_result in names)


class ReplayQueryService:
    """Resolve ready replay manifests through short-lived read sessions."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: ReplayRepositoryFactory,
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
        repository_factory: ReplayRepositoryFactory,
        outbox_writer_factory: OutboxWriterFactory,
    ) -> None:
        """Bind transaction, scoring persistence, and durable events."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory

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
            inserted = await self._repository_factory(uow.session).record_replay_view(
                request_id=request_id,
                score_id=replay.score_id,
                score_owner_account_id=replay.owner_account_id,
                viewer_account_id=viewer_account_id,
            )
            if inserted:
                await self._outbox_writer_factory(uow.session).append(
                    PendingEvent(
                        aggregate_type="score",
                        aggregate_id=str(replay.score_id),
                        event_type="score.replay-viewed.v1",
                        schema_version=1,
                        payload={
                            "score_id": replay.score_id,
                            "account_id": replay.owner_account_id,
                            "scoreboard_id": replay.scoreboard_id,
                            "request_id": str(request_id),
                        },
                        consumers=(_STATS_CONSUMER,),
                        partition_key=f"account:{replay.owner_account_id}:scoreboard:{replay.scoreboard_id}",
                    )
                )
            await uow.commit()


class RankingQueryService:
    """Read bounded leaderboard projections through canonical dimensions."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: RankingRepositoryFactory,
        cache: CacheBackend,
    ) -> None:
        """Bind transaction and scoring repository factories."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._cache = cache

    @cached(
        key_builder=lambda self, **kwargs: self._public_leaderboard_cache_key(**kwargs),
        encode=encode_json,
        decode=lambda raw: _leaderboard_scores_from_cache(raw),
        ttl_seconds=120,
        enabled=lambda _self, **kwargs: (
            kwargs["scope"].kind in {LeaderboardScopeKind.OVERALL, LeaderboardScopeKind.EXACT_MODS}
        ),
    )
    async def get_public_leaderboard(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
        scope: LeaderboardScope,
        limit: int = 50,
    ) -> tuple[LeaderboardScoreView, ...]:
        """Return public leaderboard rows, caching only shared overall/mod rows."""
        _positive_identifier("beatmap_id", beatmap_id)
        if not 1 <= limit <= 100:
            raise ValueError("leaderboard mods or limit is invalid")
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).get_public_leaderboard(
                beatmap_id=beatmap_id,
                ruleset=ruleset,
                variant=variant,
                scope=scope,
                limit=limit,
            )
        return result

    @cached(
        key_builder=lambda self, **kwargs: self._personal_leaderboard_cache_key(**kwargs),
        encode=encode_json,
        decode=lambda raw: _personal_score_from_cache(raw),
        ttl_seconds=120,
        enabled=lambda _self, **kwargs: (
            kwargs["scope"].kind in {LeaderboardScopeKind.OVERALL, LeaderboardScopeKind.EXACT_MODS}
        ),
        cache_none=True,
    )
    async def get_personal_leaderboard(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
        scope: LeaderboardScope,
        account_id: int,
    ) -> LeaderboardScoreView | None:
        """Return one account's leaderboard row with a short-lived cache."""
        _positive_identifier("beatmap_id", beatmap_id)
        _positive_identifier("account_id", account_id)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).get_personal_leaderboard(
                beatmap_id=beatmap_id,
                ruleset=ruleset,
                variant=variant,
                scope=scope,
                account_id=account_id,
            )
        return result

    async def get_combined_leaderboard(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
        scope: LeaderboardScope,
        requester_account_id: int,
        limit: int = 50,
    ) -> LeaderboardPage:
        """Compose public rows and the requester's personal row."""
        public = await self.get_public_leaderboard(
            beatmap_id=beatmap_id, ruleset=ruleset, variant=variant, scope=scope, limit=limit
        )
        personal = await self.get_personal_leaderboard(
            beatmap_id=beatmap_id,
            ruleset=ruleset,
            variant=variant,
            scope=scope,
            account_id=requester_account_id,
        )
        async with self._uow_factory() as uow:
            total_count = await self._repository_factory(uow.session).get_leaderboard_count(
                beatmap_id=beatmap_id,
                ruleset=ruleset,
                variant=variant,
                scope=scope,
            )
        return LeaderboardPage(public, personal, total_count)

    def _leaderboard_key(
        self,
        kind: str,
        beatmap_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
        scope: LeaderboardScope,
        tail: int,
        generation: str,
    ) -> str:
        dimension = scope.kind.value
        if scope.mod_acronyms is not None:
            digest = hashlib.sha256(",".join(sorted(scope.mod_acronyms)).encode()).hexdigest()[:16]
            dimension += f":{digest}"
        if scope.account_ids is not None:
            digest = hashlib.sha256(",".join(map(str, sorted(scope.account_ids))).encode()).hexdigest()[:16]
            dimension += f":{digest}"
        if scope.country_code is not None:
            dimension += f":{scope.country_code}"
        return self._cache.key(
            "scoring",
            f"leaderboard-{kind}",
            f"{dimension}:{beatmap_id}:{ruleset.value}:{variant.value}:{tail}:{generation}",
        )

    async def _public_leaderboard_cache_key(self, **kwargs: object) -> str:
        beatmap_id = cast(int, kwargs["beatmap_id"])
        generation = await self._leaderboard_generation(beatmap_id)
        return self._leaderboard_key(
            "public",
            beatmap_id,
            cast(Ruleset, kwargs["ruleset"]),
            cast(ScoreboardVariant, kwargs["variant"]),
            cast(LeaderboardScope, kwargs["scope"]),
            cast(int, kwargs["limit"]),
            generation,
        )

    async def _personal_leaderboard_cache_key(self, **kwargs: object) -> str:
        beatmap_id = cast(int, kwargs["beatmap_id"])
        generation = await self._leaderboard_generation(beatmap_id)
        return self._leaderboard_key(
            "personal",
            beatmap_id,
            cast(Ruleset, kwargs["ruleset"]),
            cast(ScoreboardVariant, kwargs["variant"]),
            cast(LeaderboardScope, kwargs["scope"]),
            cast(int, kwargs["account_id"]),
            generation,
        )

    async def _leaderboard_generation(self, beatmap_id: int) -> str:
        key = self._cache.key("scoring", "leaderboard-generation", str(beatmap_id))
        raw = await self._cache.get(key)
        if raw is None:
            return "0"
        try:
            return str(int(raw))
        except TypeError, ValueError:
            return "0"


class ScoreQueryService:
    """Read canonical score details without protocol-specific query logic."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: ScoreQueryRepositoryFactory,
    ) -> None:
        """Bind score-detail persistence dependencies."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory

    async def get(self, score_id: int, ruleset: Ruleset | None = None) -> ScoreDetailView | None:
        """Return a score, treating a ruleset mismatch as absence."""
        _positive_identifier("score_id", score_id)
        async with self._uow_factory() as uow:
            detail = await self._repository_factory(uow.session).get_score_detail(score_id)
        if detail is not None and ruleset is not None and detail.ruleset is not ruleset:
            return None
        return detail


class AccountStatisticsQueryService:
    """Read projected account statistics with explicit freshness semantics."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: AccountStatisticsRepositoryFactory,
        cache: CacheBackend,
    ) -> None:
        """Bind statistics persistence and the optional display cache."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._cache = cache

    async def get_for_submission(
        self,
        account_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant = ScoreboardVariant.VANILLA,
    ) -> AccountStatsView:
        """Read stats authoritatively for score submission before/after values."""
        return await self._load(account_id, ruleset, variant)

    @cached(
        key_builder=lambda self, account_id, ruleset, variant: self._cache.key(
            "scoring", "account-stats", f"{account_id}:{ruleset.value}:{variant.value}"
        ),
        encode=encode_json,
        decode=lambda raw: _account_stats_from_cache(raw),
        ttl_seconds=3,
    )
    async def get_for_display(
        self,
        account_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant = ScoreboardVariant.VANILLA,
    ) -> AccountStatsView:
        """Read short-lived stats suitable for online display."""
        _positive_identifier("account_id", account_id)
        return await self._load(account_id, ruleset, variant)

    async def _load(self, account_id: int, ruleset: Ruleset, variant: ScoreboardVariant) -> AccountStatsView:
        _positive_identifier("account_id", account_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).get_account_stats(account_id, ruleset, variant)


class BeatmapScoresQueryService:
    """Read projected account scores for bounded beatmap batches."""

    def __init__(
        self,
        uow_factory: Callable[[], ScoringUnitOfWork],
        repository_factory: BeatmapScoresRepositoryFactory,
    ) -> None:
        """Bind the scoring projection query dependencies."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory

    async def get_for_account(self, account_id: int, beatmap_ids: tuple[int, ...]) -> tuple[BeatmapGradeView, ...]:
        """Return projected grades, preserving absent grades as absent."""
        _positive_identifier("account_id", account_id)
        if len(beatmap_ids) > 2048 or any(identifier < 1 for identifier in beatmap_ids):
            raise ValueError("beatmap score selectors are invalid")
        if not beatmap_ids:
            return ()
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).get_beatmap_grades(account_id, beatmap_ids)


def _leaderboard_scores_from_cache(raw: bytes) -> tuple[LeaderboardScoreView, ...]:
    value = decode_json(raw)
    if not isinstance(value, list):
        raise ValueError("invalid cached leaderboard")
    return tuple(_leaderboard_score_from_mapping(item) for item in value)


def _personal_score_from_cache(raw: bytes) -> LeaderboardScoreView | None:
    value = decode_json(raw)
    if value is None:
        return None
    return _leaderboard_score_from_mapping(value)


def _leaderboard_score_from_mapping(value: object) -> LeaderboardScoreView:
    if not isinstance(value, dict):
        raise ValueError("invalid cached leaderboard score")
    return LeaderboardScoreView(
        score_id=int(value["score_id"]),
        account_id=int(value["account_id"]),
        display_name=str(value["display_name"]),
        metric_value=_decimal_cache_value(value["metric_value"]),
        max_combo=int(value["max_combo"]),
        n50=int(value["n50"]),
        n100=int(value["n100"]),
        n300=int(value["n300"]),
        nmiss=int(value["nmiss"]),
        nkatu=int(value["nkatu"]),
        ngeki=int(value["ngeki"]),
        perfect=bool(value["perfect"]),
        mods=_mods_from_cache(value["mods"]),
        rank=int(value["rank"]),
        ended_at=_datetime_cache_value(value["ended_at"]),
        has_replay=bool(value["has_replay"]),
    )


def _account_stats_from_cache(raw: bytes) -> AccountStatsView:
    value = decode_json(raw)
    if not isinstance(value, dict):
        raise ValueError("invalid cached account stats")
    return AccountStatsView(
        ranked_score=int(value["ranked_score"]),
        accuracy=_decimal_cache_value(value["accuracy"]),
        play_count=int(value["play_count"]),
        total_score=int(value["total_score"]),
        global_rank=int(value["global_rank"]) if value["global_rank"] is not None else None,
        performance=int(value["performance"]),
        country_rank=int(value["country_rank"]) if value.get("country_rank") is not None else None,
        play_time_ms=int(value.get("play_time_ms", 0)),
        total_hits=int(value.get("total_hits", 0)),
        maximum_combo=int(value.get("maximum_combo", 0)),
        replay_views=int(value.get("replay_views", 0)),
        grade_counts=cast(dict[str, int], value.get("grade_counts", {})),
    )


def _mods_from_cache(value: object) -> tuple[CanonicalMod, ...]:
    if not isinstance(value, list):
        raise ValueError("invalid cached leaderboard mods")
    mods: list[CanonicalMod] = []
    for item in value:
        if not isinstance(item, dict) or "acronym" not in item:
            raise ValueError("invalid cached leaderboard mod")
        settings = item.get("settings", {})
        if not isinstance(settings, dict):
            raise ValueError("invalid cached leaderboard mod settings")
        mods.append(CanonicalMod(str(item["acronym"]), settings))
    return tuple(mods)


def _decimal_cache_value(value: object) -> Decimal:
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(str(value["value"]))
    return Decimal(str(value))


def _datetime_cache_value(value: object) -> datetime:
    if isinstance(value, dict) and value.get("__type__") == "datetime":
        return datetime.fromisoformat(str(value["value"]))
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _positive_identifier(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _log_acceptance(
    level: str,
    event: str,
    result: AcceptedScoreResult,
    *,
    account_id: int,
    started_ns: int,
) -> None:
    log_event(
        level,
        event,
        account_id=account_id,
        score_id=result.score_id,
        attempt_id=str(result.attempt_id),
        beatmap_id=result.beatmap_id,
        beatmap_revision_id=result.beatmap_revision_id,
        scoreboard_id=result.scoreboard_id,
        mod_set_id=result.mod_set_id,
        duration_ms=duration_ms(started_ns),
    )
