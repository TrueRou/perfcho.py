"""Persist canonical score acceptance and authoritative submission validation."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import CTE

from perfcho.infra.db.enums import (
    AccountStatus,
    AttemptStatus,
    SanctionKind,
    SessionStatus,
)
from perfcho.infra.db.enums import (
    ClientFamily as DbClientFamily,
)
from perfcho.infra.db.enums import (
    Ruleset as DbRuleset,
)
from perfcho.infra.db.enums import (
    ScoreGrade as DbScoreGrade,
)
from perfcho.infra.db.enums import (
    ScoreOutcome as DbScoreOutcome,
)
from perfcho.infra.db.idempotency import CommandReceiptRepository, ReceiptClaim, ReceiptClaimState
from perfcho.infra.db.models.content import Beatmap, BeatmapRevision
from perfcho.infra.db.models.core import Account, AccountName
from perfcho.infra.db.models.moderation import Sanction
from perfcho.infra.db.models.multiplayer import (
    MultiplayerAttempt,
    MultiplayerSession,
    PlaylistRevision,
    Round,
    RoundParticipant,
    TournamentPoolItem,
)
from perfcho.infra.db.models.scoring import (
    CalculationRelease,
    PlayAttempt,
    PlayAttemptToken,
    RankingPolicy,
    Replay,
    ReplayViewEvent,
    Score,
    ScoreAttestation,
    ScoreHitStatistic,
    ScorePerformance,
    UserPlayStat,
    UserRankedStat,
)
from perfcho.modules.common import AccountUnavailable
from perfcho.modules.scoring.errors import AttemptIdempotencyConflict, MultiplayerContextRejected, ScoreRejected
from perfcho.modules.scoring.models import (
    AcceptanceClaim,
    AcceptedScoreResult,
    AccountStatsView,
    AccountSubmissionContext,
    AttemptClaim,
    BeatmapGradeView,
    BeatmapReference,
    BeatmapRevisionInfo,
    CanonicalMod,
    LeaderboardScope,
    LeaderboardScoreView,
    MultiplayerSubmissionContext,
    PlayAttemptRecord,
    PlayAttemptSubmission,
    PopulationFilter,
    ReplayReference,
    Ruleset,
    ScoreAcceptanceRecord,
    ScoreDetailView,
    ScoreDimension,
    ScoreGrade,
    ScoreOutcome,
    SoloScoreToken,
    thaw_json_mapping,
)

_RECEIPT_SCOPE = "scoring.accept"


class SqlAlchemyScoringRepository:
    """Write score facts through one caller-owned asynchronous transaction."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind score operations and command receipts to the caller session."""
        self._session = session
        self._receipts = CommandReceiptRepository(session)

    async def claim_acceptance(
        self,
        *,
        idempotency_key: str,
        request_digest: bytes,
        now: datetime,
        expires_at: datetime,
    ) -> AcceptanceClaim:
        """Claim command idempotency and deserialize an exact accepted result."""
        claim = await self._receipts.claim(
            scope=_RECEIPT_SCOPE,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
            now=now,
            expires_at=expires_at,
        )
        if claim.state is ReceiptClaimState.NEW:
            return AcceptanceClaim()
        return AcceptanceClaim(_result_from_receipt(claim))

    async def resolve_current_revision(self, reference: BeatmapReference) -> BeatmapRevisionInfo | None:
        """Resolve a current revision by canonical beatmap ID and/or Stable MD5."""
        statement = (
            select(
                Beatmap.id,
                BeatmapRevision.id.label("revision_id"),
                Beatmap.ruleset,
                Beatmap.status,
                BeatmapRevision.object_count,
                BeatmapRevision.max_combo,
            )
            .join(BeatmapRevision, BeatmapRevision.beatmap_id == Beatmap.id)
            .where(BeatmapRevision.is_current.is_(True), Beatmap.deleted_at.is_(None))
        )
        if reference.beatmap_id is not None:
            statement = statement.where(Beatmap.id == reference.beatmap_id)
        if reference.md5 is not None:
            statement = statement.where(BeatmapRevision.md5 == reference.md5)
        row = (await self._session.execute(statement.limit(1))).one_or_none()
        if row is None:
            return None
        return BeatmapRevisionInfo(
            beatmap_id=row.id,
            revision_id=row.revision_id,
            ruleset=Ruleset(row.ruleset.value),
            status=row.status.value,
            object_count=row.object_count,
            max_combo=row.max_combo,
        )

    async def claim_attempt(self, record: PlayAttemptRecord) -> AttemptClaim:
        """Claim independent play-attempt idempotency and return an accepted replay."""
        inserted_id = await self._session.scalar(
            insert(PlayAttempt)
            .values(
                id=record.attempt_id,
                account_id=record.account_id,
                beatmap_id=record.beatmap_id,
                beatmap_revision_id=record.beatmap_revision_id,
                ruleset=DbRuleset(record.ruleset.value),
                mods_details=[mod.as_json() for mod in record.mods],
                mods_acronyms=sorted(mod.acronym for mod in record.mods),
                mods_digest=record.mods_digest,
                protocol=DbClientFamily(record.source),
                idempotency_key=record.submission.idempotency_key,
                status=AttemptStatus.SUBMITTED,
                started_at=record.submission.started_at,
                ended_at=record.submission.ended_at,
                outcome=DbScoreOutcome(record.outcome.value),
                progress=record.submission.progress,
                client_metadata=thaw_json_mapping(record.submission.client_metadata),
            )
            .on_conflict_do_nothing(
                index_elements=(PlayAttempt.account_id, PlayAttempt.protocol, PlayAttempt.idempotency_key)
            )
            .returning(PlayAttempt.id)
        )
        if inserted_id is not None:
            return AttemptClaim(inserted_id)

        attempt = (
            await self._session.execute(
                select(PlayAttempt)
                .where(
                    PlayAttempt.account_id == record.account_id,
                    PlayAttempt.protocol == DbClientFamily(record.source),
                    PlayAttempt.idempotency_key == record.submission.idempotency_key,
                )
                .with_for_update()
            )
        ).scalar_one()
        expected = (
            record.beatmap_id,
            record.beatmap_revision_id,
            record.ruleset.value,
            [mod.as_json() for mod in record.mods],
            sorted(mod.acronym for mod in record.mods),
            record.mods_digest,
            record.submission.started_at,
            record.submission.ended_at,
            record.outcome.value,
            record.submission.progress,
            thaw_json_mapping(record.submission.client_metadata),
        )
        actual = (
            attempt.beatmap_id,
            attempt.beatmap_revision_id,
            attempt.ruleset.value,
            attempt.mods_details,
            attempt.mods_acronyms,
            attempt.mods_digest,
            attempt.started_at,
            attempt.ended_at,
            attempt.outcome.value if attempt.outcome is not None else None,
            attempt.progress,
            attempt.client_metadata,
        )
        if actual != expected:
            raise AttemptIdempotencyConflict("play-attempt key was reused for different facts")
        prior = await self._accepted_result_for_attempt(attempt.id)
        return AttemptClaim(attempt.id, prior)

    async def insert_score(self, record: ScoreAcceptanceRecord) -> AcceptedScoreResult:
        """Insert score, hit, optional replay, and attestation facts."""
        score = Score(
            attempt_id=record.attempt_id,
            account_id=record.account_id,
            beatmap_id=record.revision.beatmap_id,
            beatmap_revision_id=record.revision.revision_id,
            ruleset=DbRuleset(record.ruleset.value),
            mods_details=[mod.as_json() for mod in record.mods],
            mods_acronyms=sorted(mod.acronym for mod in record.mods),
            mods_digest=record.mods_digest,
            total_score=record.score.total_score,
            classic_score=record.score.classic_score,
            accuracy=record.validated.accuracy,
            max_combo=record.score.max_combo,
            grade=DbScoreGrade(record.validated.grade.value),
            outcome=DbScoreOutcome(record.score.outcome.value),
            perfect=record.score.perfect,
            client_flags=record.score.client_flags,
            online_checksum=record.score.online_checksum,
            started_at=record.attempt.started_at,
            ended_at=record.attempt.ended_at,
            processed_at=record.processed_at,
        )
        self._session.add(score)
        await self._session.flush()
        if score.id is None:
            raise RuntimeError("database did not assign a score identifier")
        score_id = score.id
        self._session.add_all(
            ScoreHitStatistic(
                score_id=score_id,
                hit_result=statistic.hit_result,
                actual=statistic.actual,
                maximum=statistic.maximum,
            )
            for statistic in record.score.hits
        )
        if record.replay is not None:
            self._session.add(
                Replay(
                    score_id=score_id,
                    format=record.replay.format,
                    sha256=record.replay.sha256,
                    size_bytes=record.replay.size_bytes,
                    storage_key=record.replay.storage_key,
                    state="ready",
                    client_version=record.replay.client_version,
                    verified_at=record.processed_at,
                )
            )
        self._session.add(
            ScoreAttestation(
                score_id=score_id,
                client_family=DbClientFamily(record.attestation.source),
                client_version=record.attestation.client_version,
                client_flags=record.attestation.client_flags,
                checksum=record.attestation.checksum,
                client_integrity_digest=record.attestation.client_integrity_digest,
                verification_state=record.attestation.verification_state,
                evidence=thaw_json_mapping(record.attestation.evidence),
            )
        )
        updated_attempt_id = await self._session.scalar(
            update(PlayAttempt)
            .where(PlayAttempt.id == record.attempt_id, PlayAttempt.status == AttemptStatus.SUBMITTED)
            .values(status=AttemptStatus.VERIFIED)
            .returning(PlayAttempt.id)
        )
        if updated_attempt_id is None:
            raise ScoreRejected("play attempt is no longer submitable")
        return AcceptedScoreResult(
            attempt_id=record.attempt_id,
            score_id=score_id,
            beatmap_id=record.revision.beatmap_id,
            beatmap_revision_id=record.revision.revision_id,
            ruleset=record.ruleset,
            mods=record.mods,
            mods_digest=record.mods_digest,
            outcome=record.score.outcome,
        )

    async def complete_acceptance(self, idempotency_key: str, result: AcceptedScoreResult) -> None:
        """Attach the exact accepted result to its command receipt."""
        await self._receipts.complete(
            scope=_RECEIPT_SCOPE,
            idempotency_key=idempotency_key,
            resource_type="score",
            resource_id=str(result.score_id),
            result_snapshot=_result_snapshot(result),
        )

    async def issue_solo_token(
        self,
        *,
        account_id: int,
        revision: BeatmapRevisionInfo,
        ruleset: Ruleset,
        started_at: datetime,
        expires_at: datetime,
    ) -> SoloScoreToken:
        """Create one numeric token matching the osu!lazer client contract."""
        token = PlayAttemptToken(
            account_id=account_id,
            beatmap_id=revision.beatmap_id,
            beatmap_revision_id=revision.revision_id,
            ruleset=DbRuleset(ruleset.value),
            protocol=DbClientFamily.LAZER,
            expires_at=expires_at,
        )
        self._session.add(token)
        await self._session.flush()
        if token.id is None:
            raise RuntimeError("database did not assign a solo score token identifier")
        return SoloScoreToken(
            token_id=token.id,
            account_id=account_id,
            beatmap_id=revision.beatmap_id,
            beatmap_revision_id=revision.revision_id,
            ruleset=ruleset,
            started_at=started_at,
            expires_at=expires_at,
        )

    async def claim_solo_token(
        self,
        token_id: int,
        *,
        account_id: int,
        beatmap_id: int,
        ruleset: Ruleset,
        at: datetime,
    ) -> SoloScoreToken:
        """Lock and return one matching unexpired solo token."""
        row = (
            await self._session.execute(
                select(PlayAttemptToken).where(PlayAttemptToken.id == token_id).with_for_update()
            )
        ).scalar_one_or_none()
        if row is None:
            raise ScoreRejected("solo score token was not found")
        if (
            row.protocol is not DbClientFamily.LAZER
            or row.account_id != account_id
            or row.beatmap_id != beatmap_id
            or row.ruleset is not DbRuleset(ruleset.value)
        ):
            raise ScoreRejected("solo score token dimensions do not match the submission")
        if row.expires_at <= at and row.score_id is None:
            raise ScoreRejected("solo score token has expired")
        return SoloScoreToken(
            token_id=row.id,
            account_id=row.account_id,
            beatmap_id=row.beatmap_id,
            beatmap_revision_id=row.beatmap_revision_id,
            ruleset=Ruleset(row.ruleset.value),
            started_at=row.created_at,
            expires_at=row.expires_at,
            score_id=row.score_id,
        )

    async def complete_solo_token(self, token_id: int, score_id: int, *, at: datetime) -> None:
        """Consume a locked token and bind it to the accepted score."""
        updated_id = await self._session.scalar(
            update(PlayAttemptToken)
            .where(
                PlayAttemptToken.id == token_id,
                PlayAttemptToken.consumed_at.is_(None),
                PlayAttemptToken.score_id.is_(None),
            )
            .values(consumed_at=at, score_id=score_id)
            .returning(PlayAttemptToken.id)
        )
        if updated_id is None:
            existing_score_id = await self._session.scalar(
                select(PlayAttemptToken.score_id).where(PlayAttemptToken.id == token_id)
            )
            if existing_score_id != score_id:
                raise ScoreRejected("solo score token was already consumed")

    async def accepted_result_for_score(self, score_id: int) -> AcceptedScoreResult | None:
        """Resolve one immutable score into the service result contract."""
        row = (
            await self._session.execute(
                select(
                    Score.attempt_id,
                    Score.id,
                    Score.beatmap_id,
                    Score.beatmap_revision_id,
                    Score.ruleset,
                    Score.mods_details,
                    Score.mods_digest,
                    Score.outcome,
                ).where(Score.id == score_id)
            )
        ).one_or_none()
        if row is None:
            return None
        return AcceptedScoreResult(
            attempt_id=row.attempt_id,
            score_id=row.id,
            beatmap_id=row.beatmap_id,
            beatmap_revision_id=row.beatmap_revision_id,
            ruleset=Ruleset(row.ruleset.value),
            mods=_canonical_mods(row.mods_details),
            mods_digest=row.mods_digest,
            outcome=ScoreOutcome(row.outcome.value),
        )

    async def get_replay(self, score_id: int) -> ReplayReference | None:
        """Resolve one ready replay with score ownership and ruleset."""
        row = (
            await self._session.execute(
                select(
                    Replay.score_id,
                    Score.account_id,
                    Score.ruleset,
                    Score.mods_digest,
                    Replay.storage_key,
                    Replay.size_bytes,
                    Replay.format,
                )
                .join(Score, Score.id == Replay.score_id)
                .where(Replay.score_id == score_id, Replay.state == "ready")
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            return None
        return ReplayReference(
            score_id=row.score_id,
            owner_account_id=row.account_id,
            ruleset=Ruleset(row.ruleset.value),
            mods_digest=row.mods_digest,
            storage_key=row.storage_key,
            size_bytes=row.size_bytes,
            format=row.format,
        )

    async def record_replay_view(
        self,
        *,
        request_id: uuid.UUID,
        score_id: int,
        score_owner_account_id: int,
        viewer_account_id: int | None,
    ) -> bool:
        """Insert one replay view fact idempotently by request ID."""
        inserted = await self._session.scalar(
            insert(ReplayViewEvent)
            .values(
                request_id=request_id,
                score_id=score_id,
                score_owner_account_id=score_owner_account_id,
                viewer_account_id=viewer_account_id,
            )
            .on_conflict_do_nothing(index_elements=(ReplayViewEvent.request_id,))
            .returning(ReplayViewEvent.id)
        )
        return inserted is not None

    async def get_score_detail(self, score_id: int) -> ScoreDetailView | None:
        """Read immutable score facts and its live classic-score position."""
        pp = (
            select(ScorePerformance.pp)
            .join(CalculationRelease, CalculationRelease.id == ScorePerformance.release_id)
            .where(
                ScorePerformance.score_id == Score.id,
                CalculationRelease.ruleset == Score.ruleset,
                CalculationRelease.active.is_(True),
            )
            .order_by(CalculationRelease.version, CalculationRelease.id)
            .limit(1)
            .correlate(Score)
            .scalar_subquery()
        )
        row = (
            await self._session.execute(
                select(
                    Score,
                    AccountName.display_name,
                    Account.country_code,
                    pp.label("pp"),
                    Replay.score_id.label("replay_score_id"),
                )
                .join(Account, Account.id == Score.account_id)
                .join(AccountName, and_(AccountName.account_id == Score.account_id, AccountName.ended_at.is_(None)))
                .outerjoin(Replay, and_(Replay.score_id == Score.id, Replay.state == "ready"))
                .where(Score.id == score_id)
            )
        ).one_or_none()
        if row is None:
            return None
        score = row.Score
        hits = await self._score_hits_with_maximum({score_id})
        statistics, maximum_statistics = hits.get(score_id, ({}, {}))
        position = await self._score_position(score)
        return ScoreDetailView(
            score_id=score.id,
            account_id=score.account_id,
            display_name=row.display_name,
            country_code=row.country_code,
            beatmap_id=score.beatmap_id,
            ruleset=Ruleset(score.ruleset.value),
            total_score=score.total_score,
            classic_score=score.classic_score,
            accuracy=score.accuracy,
            max_combo=score.max_combo,
            grade=ScoreGrade(score.grade.value),
            outcome=ScoreOutcome(score.outcome.value),
            mods=_canonical_mods(score.mods_details),
            statistics=statistics,
            maximum_statistics=maximum_statistics,
            started_at=score.started_at,
            ended_at=score.ended_at,
            has_replay=row.replay_score_id is not None,
            ranked=score.outcome is DbScoreOutcome.PASSED,
            pp=row.pp,
            position=position,
        )

    async def _score_position(self, score: Score) -> int | None:
        if score.outcome is not DbScoreOutcome.PASSED:
            return None
        better_personal = await self._session.scalar(
            select(func.count())
            .select_from(Score)
            .where(
                Score.account_id == score.account_id,
                Score.beatmap_id == score.beatmap_id,
                Score.ruleset == score.ruleset,
                Score.outcome == DbScoreOutcome.PASSED,
                or_(
                    Score.classic_score > score.classic_score,
                    and_(
                        Score.classic_score == score.classic_score,
                        or_(
                            Score.ended_at < score.ended_at, and_(Score.ended_at == score.ended_at, Score.id < score.id)
                        ),
                    ),
                ),
            )
        )
        if better_personal:
            return None
        ranked = self._ranked_scores(score.beatmap_id, Ruleset(score.ruleset.value), LeaderboardScope.overall())
        return int(await self._session.scalar(select(ranked.c.rank).where(ranked.c.score_id == score.id)) or 0) or None

    async def get_public_leaderboard(
        self, *, beatmap_id: int, ruleset: Ruleset, scope: LeaderboardScope, limit: int
    ) -> tuple[LeaderboardScoreView, ...]:
        """Return live passed-score personal bests ranked by classic score."""
        return await self._query_leaderboard_rows(
            beatmap_id=beatmap_id, ruleset=ruleset, scope=scope, limit=limit, account_id=None
        )

    async def get_personal_leaderboard(
        self, *, beatmap_id: int, ruleset: Ruleset, scope: LeaderboardScope, account_id: int
    ) -> LeaderboardScoreView | None:
        """Return the account row using the common leaderboard query."""
        rows = await self._query_leaderboard_rows(
            beatmap_id=beatmap_id, ruleset=ruleset, scope=scope, limit=1, account_id=account_id
        )
        return rows[0] if rows else None

    async def _query_leaderboard_rows(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        scope: LeaderboardScope,
        limit: int,
        account_id: int | None,
    ) -> tuple[LeaderboardScoreView, ...]:
        """Run the bounded live leaderboard query for public or one-account rows."""
        ranked = self._ranked_scores(beatmap_id, ruleset, scope)
        statement = select(ranked)
        if account_id is not None:
            statement = statement.where(ranked.c.account_id == account_id)
        rows = (
            await self._session.execute(
                statement.join(
                    AccountName, and_(AccountName.account_id == ranked.c.account_id, AccountName.ended_at.is_(None))
                )
                .outerjoin(Replay, and_(Replay.score_id == ranked.c.score_id, Replay.state == "ready"))
                .add_columns(AccountName.display_name, Replay.score_id.label("replay_score_id"))
                .order_by(ranked.c.rank)
                .limit(limit)
            )
        ).all()
        score_ids = {row.score_id for row in rows}
        hits = await self._score_hits(score_ids)
        return tuple(_leaderboard_view(row._tuple(), ruleset=ruleset, hits=hits.get(row.score_id, {})) for row in rows)

    def _ranked_scores(self, beatmap_id: int, ruleset: Ruleset, scope: LeaderboardScope) -> CTE:
        order = (Score.classic_score.desc(), Score.ended_at.asc(), Score.id.asc())
        statement = (
            select(
                Score.id.label("score_id"),
                Score.account_id,
                Score.classic_score.label("metric_value"),
                Score.max_combo,
                Score.perfect,
                Score.ended_at,
                Score.mods_details,
                func.row_number().over(partition_by=Score.account_id, order_by=order).label("account_position"),
            )
            .join(Account, Account.id == Score.account_id)
            .where(
                Score.beatmap_id == beatmap_id,
                Score.ruleset == DbRuleset(ruleset.value),
                Score.outcome == DbScoreOutcome.PASSED,
            )
        )
        if scope.dimension is ScoreDimension.EXACT_MODS:
            statement = statement.where(Score.mods_acronyms == sorted(scope.mod_acronyms or ()))
        if scope.population is PopulationFilter.FRIENDS:
            statement = statement.where(Score.account_id.in_(scope.account_ids or ()))
        elif scope.population is PopulationFilter.COUNTRY:
            statement = statement.where(Account.country_code == scope.country_code)
        personal_bests = statement.cte("live_score_personal_bests")
        return (
            select(
                personal_bests.c.score_id,
                personal_bests.c.account_id,
                personal_bests.c.metric_value,
                personal_bests.c.max_combo,
                personal_bests.c.perfect,
                personal_bests.c.ended_at,
                personal_bests.c.mods_details,
                func.row_number()
                .over(
                    order_by=(
                        personal_bests.c.metric_value.desc(),
                        personal_bests.c.ended_at.asc(),
                        personal_bests.c.score_id.asc(),
                    )
                )
                .label("rank"),
            )
            .where(personal_bests.c.account_position == 1)
            .cte("live_ranked_scores")
        )

    async def get_leaderboard_count(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        scope: LeaderboardScope,
    ) -> int:
        """Return the complete account count after exact-mod setting variants merge."""
        rows = await self._query_leaderboard_rows(
            beatmap_id=beatmap_id,
            ruleset=ruleset,
            scope=scope,
            limit=100,
            account_id=None,
        )
        if len(rows) < 100:
            return len(rows)
        ranked = self._ranked_scores(beatmap_id, ruleset, scope)
        return int(await self._session.scalar(select(func.count()).select_from(ranked)) or 0)

    async def get_account_stats(
        self,
        account_id: int,
        ruleset: Ruleset,
    ) -> AccountStatsView:
        """Compose factual play totals with the first active ruleset policy."""
        row = (
            await self._session.execute(
                select(
                    RankingPolicy.id.label("policy_id"),
                    RankingPolicy.configuration,
                    UserPlayStat.play_count,
                    UserPlayStat.total_score,
                    UserPlayStat.play_time_ms,
                    UserPlayStat.total_hits,
                    UserPlayStat.max_combo,
                    UserPlayStat.replay_views,
                    UserRankedStat.ranked_score,
                    UserRankedStat.accuracy,
                    UserRankedStat.performance,
                    UserRankedStat.grade_counts,
                    Account.country_code,
                )
                .select_from(UserPlayStat)
                .outerjoin(
                    RankingPolicy,
                    (RankingPolicy.ruleset == UserPlayStat.ruleset) & RankingPolicy.active.is_(True),
                )
                .outerjoin(
                    UserRankedStat,
                    (UserRankedStat.policy_id == RankingPolicy.id) & (UserRankedStat.account_id == account_id),
                )
                .outerjoin(Account, Account.id == UserPlayStat.account_id)
                .where(
                    UserPlayStat.account_id == account_id,
                    UserPlayStat.ruleset == DbRuleset(ruleset.value),
                )
                .order_by(RankingPolicy.code)
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            return AccountStatsView(0, Decimal(0), 0, 0, 0)
        global_rank = 0
        country_rank: int | None = None
        if row.policy_id is not None:
            metric = str(row.configuration.get("metric", "classic_score"))
            rank_value = row.performance if metric == "pp" else row.ranked_score
            if rank_value is not None and rank_value > 0:
                rank_filter = (
                    UserRankedStat.performance > rank_value
                    if metric == "pp"
                    else UserRankedStat.ranked_score > rank_value
                )
                higher = await self._session.scalar(
                    select(func.count())
                    .select_from(UserRankedStat)
                    .where(
                        UserRankedStat.policy_id == row.policy_id,
                        rank_filter,
                    )
                )
                global_rank = int(higher or 0) + 1
                country_code = await self._session.scalar(select(Account.country_code).where(Account.id == account_id))
                if country_code:
                    country_higher = await self._session.scalar(
                        select(func.count())
                        .select_from(UserRankedStat)
                        .join(Account, Account.id == UserRankedStat.account_id)
                        .where(
                            UserRankedStat.policy_id == row.policy_id,
                            Account.country_code == country_code,
                            rank_filter,
                        )
                    )
                    country_rank = int(country_higher or 0) + 1
        return AccountStatsView(
            ranked_score=row.ranked_score or 0,
            accuracy=row.accuracy or Decimal(0),
            play_count=row.play_count or 0,
            total_score=row.total_score or 0,
            global_rank=global_rank,
            performance=int(row.performance) if row.performance is not None else 0,
            country_rank=country_rank,
            play_time_ms=getattr(row, "play_time_ms", 0) or 0,
            total_hits=getattr(row, "total_hits", 0) or 0,
            maximum_combo=getattr(row, "max_combo", 0) or 0,
            replay_views=getattr(row, "replay_views", 0) or 0,
            grade_counts=getattr(row, "grade_counts", {}) or {},
        )

    async def get_beatmap_grades(
        self,
        account_id: int,
        beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapGradeView, ...]:
        """Return each ruleset's best passed classic-score grade per beatmap."""
        candidates = (
            select(
                Score.beatmap_id,
                Score.ruleset,
                Score.grade,
                func.row_number()
                .over(
                    partition_by=(Score.beatmap_id, Score.ruleset),
                    order_by=(Score.classic_score.desc(), Score.ended_at.asc(), Score.id.asc()),
                )
                .label("position"),
            )
            .where(
                Score.account_id == account_id,
                Score.beatmap_id.in_(set(beatmap_ids)),
                Score.outcome == DbScoreOutcome.PASSED,
            )
            .cte("account_beatmap_grades")
        )
        rows = (
            await self._session.execute(
                select(candidates.c.beatmap_id, candidates.c.ruleset, candidates.c.grade)
                .where(candidates.c.position == 1)
                .order_by(candidates.c.beatmap_id, candidates.c.ruleset)
            )
        ).all()
        return tuple(
            BeatmapGradeView(
                beatmap_id=row.beatmap_id,
                ruleset=Ruleset(row.ruleset.value),
                grade=ScoreGrade(row.grade.value),
            )
            for row in rows
        )

    async def _score_hits(self, score_ids: set[int]) -> dict[int, dict[str, int]]:
        if not score_ids:
            return {}
        rows = (
            await self._session.execute(
                select(
                    ScoreHitStatistic.score_id,
                    ScoreHitStatistic.hit_result,
                    ScoreHitStatistic.actual,
                ).where(ScoreHitStatistic.score_id.in_(score_ids))
            )
        ).all()
        result: dict[int, dict[str, int]] = {}
        for row in rows:
            result.setdefault(row.score_id, {})[row.hit_result] = row.actual
        return result

    async def _score_hits_with_maximum(self, score_ids: set[int]) -> dict[int, tuple[dict[str, int], dict[str, int]]]:
        if not score_ids:
            return {}
        rows = (
            await self._session.execute(
                select(
                    ScoreHitStatistic.score_id,
                    ScoreHitStatistic.hit_result,
                    ScoreHitStatistic.actual,
                    ScoreHitStatistic.maximum,
                ).where(ScoreHitStatistic.score_id.in_(score_ids))
            )
        ).all()
        result: dict[int, tuple[dict[str, int], dict[str, int]]] = {}
        for row in rows:
            actual, maximum = result.setdefault(row.score_id, ({}, {}))
            actual[row.hit_result] = row.actual
            if row.maximum is not None:
                maximum[row.hit_result] = row.maximum
        return result

    async def _accepted_result_for_attempt(self, attempt_id: uuid.UUID) -> AcceptedScoreResult | None:
        row = (
            await self._session.execute(
                select(
                    Score.id,
                    Score.attempt_id,
                    Score.beatmap_id,
                    Score.beatmap_revision_id,
                    Score.ruleset,
                    Score.mods_details,
                    Score.mods_digest,
                    Score.outcome,
                ).where(Score.attempt_id == attempt_id)
            )
        ).one_or_none()
        if row is None:
            return None
        return AcceptedScoreResult(
            attempt_id=row.attempt_id,
            score_id=row.id,
            beatmap_id=row.beatmap_id,
            beatmap_revision_id=row.beatmap_revision_id,
            ruleset=Ruleset(row.ruleset.value),
            mods=_canonical_mods(row.mods_details),
            mods_digest=row.mods_digest,
            outcome=ScoreOutcome(row.outcome.value),
        )


class SqlAlchemyAccountSubmissionValidator:
    """Validate active account lifecycle and score-blocking sanctions."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind authoritative account validation to the scoring transaction."""
        self._session = session

    async def validate(self, account_id: int, *, at: datetime) -> AccountSubmissionContext:
        """Require an active account without an active restriction."""
        row = (
            await self._session.execute(
                select(Account.id, Account.status, Account.country_code)
                .where(Account.id == account_id)
                .with_for_update(read=True)
            )
        ).one_or_none()
        if row is None or row.status is not AccountStatus.ACTIVE:
            raise AccountUnavailable("account cannot submit scores")
        restricted = await self._session.scalar(
            select(Sanction.id)
            .where(
                Sanction.subject_account_id == account_id,
                Sanction.kind == SanctionKind.RESTRICTION,
                Sanction.revoked_at.is_(None),
                Sanction.starts_at <= at,
                or_(Sanction.ends_at.is_(None), Sanction.ends_at > at),
            )
            .limit(1)
        )
        if restricted is not None:
            raise AccountUnavailable("account cannot submit scores")
        return AccountSubmissionContext(row.id, row.country_code)


class SqlAlchemyMultiplayerSubmissionValidator:
    """Validate and bind one authoritative multiplayer score opportunity."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind multiplayer validation to the scoring transaction."""
        self._session = session

    async def validate(
        self,
        context: MultiplayerSubmissionContext,
        *,
        account_id: int,
        revision: BeatmapRevisionInfo,
        ruleset: Ruleset,
        mods: tuple[CanonicalMod, ...],
        mods_digest: bytes,
        attempt: PlayAttemptSubmission,
        at: datetime,
    ) -> None:
        """Lock and validate one unconsumed multiplayer attempt and frozen dimensions."""
        attempt_row = await self._locked_attempt(context)
        if (
            attempt_row.account_id != account_id
            or attempt_row.status not in {AttemptStatus.ISSUED, AttemptStatus.STARTED}
            or attempt_row.expires_at <= at
            or attempt_row.score_id is not None
        ):
            raise MultiplayerContextRejected("multiplayer attempt is unavailable")
        if attempt_row.round_id is not None:
            lifecycle = (
                await self._session.execute(
                    select(Round, MultiplayerSession)
                    .join(MultiplayerSession, MultiplayerSession.id == Round.session_id)
                    .where(Round.id == attempt_row.round_id)
                    .with_for_update(of=(Round, MultiplayerSession))
                )
            ).one_or_none()
            if lifecycle is None:
                raise MultiplayerContextRejected("multiplayer round is unavailable")
            round_row, session = lifecycle
            if round_row.status not in {"in_progress", "completed", "aborted"} or session.status not in {
                SessionStatus.ACTIVE,
                SessionStatus.COMPLETED,
                SessionStatus.ABORTED,
            }:
                raise MultiplayerContextRejected("multiplayer round or session is not scoreable")
            if round_row.started_at is None or attempt.started_at < round_row.started_at:
                raise MultiplayerContextRejected("score started before its multiplayer round")
            if round_row.ended_at is not None and attempt.ended_at > round_row.ended_at:
                raise MultiplayerContextRejected("score ended after its multiplayer round")
            if round_row.ended_at is None and attempt.ended_at > at:
                raise MultiplayerContextRejected("score ended in the future")
        dimensions = await self._submission_dimensions(attempt_row)
        if dimensions is None:
            raise MultiplayerContextRejected("multiplayer attempt has no frozen scoring dimensions")
        expected_revision_id, expected_ruleset, expected_mods_details, expected_mods_digest = dimensions
        if expected_revision_id != revision.revision_id or expected_ruleset is not DbRuleset(ruleset.value):
            raise MultiplayerContextRejected("multiplayer attempt dimensions do not match the score")
        if expected_mods_details != [mod.as_json() for mod in mods] or expected_mods_digest != mods_digest:
            raise MultiplayerContextRejected("multiplayer attempt mods do not match the score")

    async def bind_score(
        self,
        context: MultiplayerSubmissionContext,
        *,
        play_attempt_id: uuid.UUID,
        score_id: int,
        at: datetime,
    ) -> None:
        """Consume and bind a previously validated attempt in the same transaction."""
        bound_id = await self._session.scalar(
            update(MultiplayerAttempt)
            .where(
                MultiplayerAttempt.id == context.attempt_id,
                MultiplayerAttempt.token_digest == context.token_digest,
                MultiplayerAttempt.status.in_((AttemptStatus.ISSUED, AttemptStatus.STARTED)),
                MultiplayerAttempt.expires_at > at,
                MultiplayerAttempt.score_id.is_(None),
            )
            .values(
                play_attempt_id=play_attempt_id,
                score_id=score_id,
                status=AttemptStatus.VERIFIED,
                consumed_at=at,
            )
            .returning(MultiplayerAttempt.id)
        )
        if bound_id is None:
            raise MultiplayerContextRejected("multiplayer attempt was already consumed")

    async def _locked_attempt(self, context: MultiplayerSubmissionContext) -> MultiplayerAttempt:
        attempt = (
            await self._session.execute(
                select(MultiplayerAttempt)
                .where(
                    MultiplayerAttempt.id == context.attempt_id,
                    MultiplayerAttempt.token_digest == context.token_digest,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if attempt is None:
            raise MultiplayerContextRejected("multiplayer attempt is unavailable")
        return attempt

    async def _submission_dimensions(
        self, attempt: MultiplayerAttempt
    ) -> tuple[int, DbRuleset, list[dict[str, object]], bytes] | None:
        if attempt.playlist_revision_id is not None:
            playlist_row = (
                await self._session.execute(
                    select(
                        PlaylistRevision.beatmap_revision_id,
                        PlaylistRevision.ruleset,
                        PlaylistRevision.required_mods_details,
                        PlaylistRevision.required_mods_digest,
                    ).where(PlaylistRevision.id == attempt.playlist_revision_id)
                )
            ).one_or_none()
            if playlist_row is None:
                return None
            return playlist_row._tuple()
        if attempt.round_id is None:
            return None
        round_row = (
            await self._session.execute(
                select(
                    PlaylistRevision.beatmap_revision_id,
                    PlaylistRevision.ruleset,
                    PlaylistRevision.required_mods_details,
                    PlaylistRevision.required_mods_digest,
                    RoundParticipant.mods_details.label("participant_mods_details"),
                    RoundParticipant.mods_digest.label("participant_mods_digest"),
                    TournamentPoolItem.beatmap_revision_id.label("pool_revision_id"),
                    TournamentPoolItem.ruleset.label("pool_ruleset"),
                    TournamentPoolItem.mods_details.label("pool_mods_details"),
                    TournamentPoolItem.mods_digest.label("pool_mods_digest"),
                )
                .select_from(Round)
                .outerjoin(PlaylistRevision, PlaylistRevision.id == Round.playlist_revision_id)
                .outerjoin(
                    RoundParticipant,
                    and_(
                        RoundParticipant.round_id == Round.id,
                        RoundParticipant.account_id == attempt.account_id,
                    ),
                )
                .outerjoin(TournamentPoolItem, TournamentPoolItem.id == Round.tournament_pool_item_id)
                .where(Round.id == attempt.round_id)
            )
        ).one_or_none()
        if round_row is None:
            return None
        beatmap_revision_id = round_row.beatmap_revision_id
        playlist_ruleset = round_row.ruleset
        required_mods_details = round_row.required_mods_details
        required_mods_digest = round_row.required_mods_digest
        participant_mods_details = round_row.participant_mods_details
        participant_mods_digest = round_row.participant_mods_digest
        pool_revision_id = round_row.pool_revision_id
        pool_ruleset = round_row.pool_ruleset
        pool_mods_details = round_row.pool_mods_details
        pool_mods_digest = round_row.pool_mods_digest
        if beatmap_revision_id is not None:
            if playlist_ruleset is None or required_mods_details is None or required_mods_digest is None:
                return None
            return (
                beatmap_revision_id,
                playlist_ruleset,
                participant_mods_details or required_mods_details,
                participant_mods_digest or required_mods_digest,
            )
        if pool_revision_id is None or pool_ruleset is None or pool_mods_details is None or pool_mods_digest is None:
            return None
        return (
            pool_revision_id,
            pool_ruleset,
            participant_mods_details or pool_mods_details,
            participant_mods_digest or pool_mods_digest,
        )


class SqlAlchemyScoringAcceptanceRepository(SqlAlchemyScoringRepository):
    """Expose score acceptance persistence as a dedicated capability."""


class SqlAlchemyReplayRepository(SqlAlchemyScoringRepository):
    """Expose replay persistence as a dedicated capability."""


class SqlAlchemyRankingRepository(SqlAlchemyScoringRepository):
    """Expose ranking projections as a dedicated capability."""


class SqlAlchemyScoreQueryRepository(SqlAlchemyScoringRepository):
    """Expose canonical score detail queries as a dedicated capability."""


class SqlAlchemyAccountStatisticsRepository(SqlAlchemyScoringRepository):
    """Expose account statistics projections as a dedicated capability."""


class SqlAlchemyBeatmapScoresRepository(SqlAlchemyScoringRepository):
    """Expose beatmap scores projections as a dedicated capability."""


def _result_snapshot(result: AcceptedScoreResult) -> dict[str, object]:
    return {
        "attempt_id": str(result.attempt_id),
        "score_id": result.score_id,
        "beatmap_id": result.beatmap_id,
        "beatmap_revision_id": result.beatmap_revision_id,
        "ruleset": result.ruleset.value,
        "mods": [mod.as_json() for mod in result.mods],
        "mods_digest": result.mods_digest.hex(),
        "outcome": result.outcome.value,
    }


def _leaderboard_view(
    row: tuple[int, int, int, int, bool, datetime, object, int, str, int | None],
    *,
    ruleset: Ruleset,
    hits: dict[str, int],
) -> LeaderboardScoreView:
    (
        score_id,
        account_id,
        raw_metric_value,
        max_combo,
        perfect,
        ended_at,
        canonical_mods,
        rank,
        display_name,
        replay_score_id,
    ) = row
    metric_value = Decimal(raw_metric_value)
    if ruleset is Ruleset.OSU:
        n300, n100, n50 = hits.get("great", 0), hits.get("ok", 0), hits.get("meh", 0)
        ngeki = nkatu = 0
    elif ruleset is Ruleset.TAIKO:
        n300, n100, n50 = hits.get("great", 0), hits.get("ok", 0), 0
        ngeki = nkatu = 0
    elif ruleset is Ruleset.FRUITS:
        n300 = hits.get("great", 0)
        n100 = hits.get("large_tick_hit", 0)
        n50 = hits.get("small_tick_hit", 0)
        ngeki = hits.get("large_tick_miss", 0)
        nkatu = hits.get("small_tick_miss", 0)
    else:
        n300, n100, n50 = hits.get("great", 0), hits.get("ok", 0), hits.get("meh", 0)
        ngeki, nkatu = hits.get("perfect", 0), hits.get("good", 0)
    return LeaderboardScoreView(
        score_id=score_id,
        account_id=account_id,
        display_name=display_name,
        metric_value=metric_value,
        max_combo=max_combo,
        n50=n50,
        n100=n100,
        n300=n300,
        nmiss=hits.get("miss", 0),
        nkatu=nkatu,
        ngeki=ngeki,
        perfect=perfect,
        mods=_canonical_mods(canonical_mods),
        rank=rank,
        ended_at=ended_at,
        has_replay=replay_score_id is not None,
    )


def _canonical_mods(value: object) -> tuple[CanonicalMod, ...]:
    if not isinstance(value, list):
        raise RuntimeError("persisted canonical mods must be a list")
    mods: list[CanonicalMod] = []
    for item in value:
        if not isinstance(item, dict) or "acronym" not in item:
            raise RuntimeError("persisted canonical mod is invalid")
        settings = item.get("settings", {})
        if not isinstance(settings, dict):
            raise RuntimeError("persisted canonical mod settings are invalid")
        mods.append(CanonicalMod(str(item["acronym"]), settings))
    return tuple(mods)


def _result_from_receipt(claim: ReceiptClaim) -> AcceptedScoreResult:
    value = claim.result_snapshot
    try:
        return AcceptedScoreResult(
            attempt_id=uuid.UUID(str(value["attempt_id"])),
            score_id=_receipt_integer(value["score_id"]),
            beatmap_id=_receipt_integer(value["beatmap_id"]),
            beatmap_revision_id=_receipt_integer(value["beatmap_revision_id"]),
            ruleset=Ruleset(str(value["ruleset"])),
            mods=_canonical_mods(value["mods"]),
            mods_digest=bytes.fromhex(str(value["mods_digest"])),
            outcome=ScoreOutcome(str(value["outcome"])),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("score receipt contains an invalid result") from error


def _receipt_integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("score receipt identifier must be an integer")
    return value
