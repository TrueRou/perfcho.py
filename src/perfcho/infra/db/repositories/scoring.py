"""Persist canonical score acceptance and authoritative submission validation."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import Select

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
    ScoreboardVariant as DbScoreboardVariant,
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
    LeaderboardEntry,
    ModSet,
    PlayAttempt,
    RankingPolicy,
    Replay,
    ReplayViewEvent,
    Score,
    ScoreAttestation,
    Scoreboard,
    ScoreHitStatistic,
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
    LeaderboardPage,
    LeaderboardScoreView,
    ModSetInfo,
    MultiplayerSubmissionContext,
    NormalizedModSet,
    PlayAttemptRecord,
    PlayAttemptSubmission,
    ReplayReference,
    Ruleset,
    ScoreAcceptanceRecord,
    ScoreboardInfo,
    ScoreboardVariant,
    ScoreGrade,
    ScoreOutcome,
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

    async def get_scoreboard(self, ruleset: Ruleset, variant: ScoreboardVariant) -> ScoreboardInfo | None:
        """Resolve one active canonical scoreboard by gameplay dimensions."""
        row = (
            await self._session.execute(
                select(Scoreboard.id, Scoreboard.code, Scoreboard.ruleset, Scoreboard.variant)
                .where(
                    Scoreboard.ruleset == DbRuleset(ruleset.value),
                    Scoreboard.variant == DbScoreboardVariant(variant.value),
                    Scoreboard.active.is_(True),
                )
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            return None
        return ScoreboardInfo(
            scoreboard_id=row.id,
            code=row.code,
            ruleset=Ruleset(row.ruleset.value),
            variant=ScoreboardVariant(row.variant.value),
        )

    async def get_or_create_mod_set(self, scoreboard_id: int, normalized: NormalizedModSet) -> ModSetInfo:
        """Upsert a deterministic canonical mod set for one scoreboard."""
        mod_set_id = await self._session.scalar(
            insert(ModSet)
            .values(
                scoreboard_id=scoreboard_id,
                canonical=list(normalized.canonical),
                canonical_digest=normalized.canonical_digest,
                legacy_bits=normalized.legacy_bits,
            )
            .on_conflict_do_nothing(index_elements=(ModSet.scoreboard_id, ModSet.canonical_digest))
            .returning(ModSet.id)
        )
        if mod_set_id is None:
            mod_set_id = await self._session.scalar(
                select(ModSet.id).where(
                    ModSet.scoreboard_id == scoreboard_id,
                    ModSet.canonical_digest == normalized.canonical_digest,
                )
            )
        if mod_set_id is None:
            raise RuntimeError("canonical mod set conflict did not resolve")
        return ModSetInfo(
            mod_set_id=mod_set_id,
            scoreboard_id=scoreboard_id,
            canonical=normalized.canonical,
            canonical_digest=normalized.canonical_digest,
            legacy_bits=normalized.legacy_bits,
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
                scoreboard_id=record.scoreboard_id,
                mod_set_id=record.mod_set_id,
                protocol=DbClientFamily(record.protocol.value),
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
                    PlayAttempt.protocol == DbClientFamily(record.protocol.value),
                    PlayAttempt.idempotency_key == record.submission.idempotency_key,
                )
                .with_for_update()
            )
        ).scalar_one()
        expected = (
            record.beatmap_id,
            record.beatmap_revision_id,
            record.scoreboard_id,
            record.mod_set_id,
            record.submission.started_at,
            record.submission.ended_at,
            record.outcome.value,
            record.submission.progress,
            thaw_json_mapping(record.submission.client_metadata),
        )
        actual = (
            attempt.beatmap_id,
            attempt.beatmap_revision_id,
            attempt.scoreboard_id,
            attempt.mod_set_id,
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
        """Insert score, hit, replay, and attestation facts."""
        score = Score(
            attempt_id=record.attempt_id,
            account_id=record.account_id,
            beatmap_id=record.revision.beatmap_id,
            beatmap_revision_id=record.revision.revision_id,
            scoreboard_id=record.scoreboard.scoreboard_id,
            mod_set_id=record.mod_set.mod_set_id,
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
                client_family=DbClientFamily(record.attestation.client_family.value),
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
            scoreboard_id=record.scoreboard.scoreboard_id,
            mod_set_id=record.mod_set.mod_set_id,
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

    async def get_replay(self, score_id: int) -> ReplayReference | None:
        """Resolve one ready replay with score ownership and ruleset."""
        row = (
            await self._session.execute(
                select(
                    Replay.score_id,
                    Score.account_id,
                    Score.scoreboard_id,
                    Scoreboard.ruleset,
                    Replay.storage_key,
                    Replay.size_bytes,
                    Replay.format,
                )
                .join(Score, Score.id == Replay.score_id)
                .join(Scoreboard, Scoreboard.id == Score.scoreboard_id)
                .where(Replay.score_id == score_id, Replay.state == "ready")
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            return None
        return ReplayReference(
            score_id=row.score_id,
            owner_account_id=row.account_id,
            scoreboard_id=row.scoreboard_id,
            ruleset=Ruleset(row.ruleset.value),
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

    async def get_leaderboard(
        self,
        *,
        beatmap_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
        leaderboard_type: int,
        legacy_mod_bits: int,
        requester_account_id: int,
        friend_account_ids: tuple[int, ...],
        limit: int,
    ) -> LeaderboardPage:
        """Read one Stable page from active ranking-policy projections."""
        scoreboard_id = await self._session.scalar(
            select(Scoreboard.id).where(
                Scoreboard.ruleset == DbRuleset(ruleset.value),
                Scoreboard.variant == DbScoreboardVariant(variant.value),
                Scoreboard.active.is_(True),
            )
        )
        if scoreboard_id is None:
            return LeaderboardPage((), None)
        policy_id = await self._session.scalar(
            select(RankingPolicy.id).where(
                RankingPolicy.scoreboard_id == scoreboard_id,
                RankingPolicy.active.is_(True),
                RankingPolicy.is_default.is_(True),
            )
        )
        if policy_id is None:
            return LeaderboardPage((), None)

        scope = "exact_mods" if leaderboard_type == 2 else "overall"
        filter_mod_set_id = None
        if scope == "exact_mods":
            filter_mod_set_ids = tuple(
                await self._session.scalars(
                    select(ModSet.id).where(
                        ModSet.scoreboard_id == scoreboard_id,
                        ModSet.legacy_bits == legacy_mod_bits,
                    )
                )
            )
            if not filter_mod_set_ids:
                return LeaderboardPage((), None)
            candidate_order = (
                LeaderboardEntry.metric_value.desc(),
                LeaderboardEntry.tie_break_value.desc(),
                LeaderboardEntry.score_id.asc(),
            )
            candidates = (
                select(
                    LeaderboardEntry.id.label("entry_id"),
                    func.row_number()
                    .over(partition_by=LeaderboardEntry.account_id, order_by=candidate_order)
                    .label("account_position"),
                )
                .where(
                    LeaderboardEntry.policy_id == policy_id,
                    LeaderboardEntry.beatmap_id == beatmap_id,
                    LeaderboardEntry.scope == scope,
                    LeaderboardEntry.filter_mod_set_id.in_(filter_mod_set_ids),
                )
                .subquery()
            )
            selected_entries = select(candidates.c.entry_id).where(candidates.c.account_position == 1)
            filters: list[ColumnElement[bool]] = [LeaderboardEntry.id.in_(selected_entries)]
        else:
            filters = [
                LeaderboardEntry.policy_id == policy_id,
                LeaderboardEntry.beatmap_id == beatmap_id,
                LeaderboardEntry.scope == scope,
                LeaderboardEntry.filter_mod_set_id == filter_mod_set_id,
            ]
        if leaderboard_type == 3:
            visible_accounts = tuple(dict.fromkeys((*friend_account_ids, requester_account_id)))
            filters.append(LeaderboardEntry.account_id.in_(visible_accounts))
        elif leaderboard_type == 4:
            country_code = await self._session.scalar(
                select(Account.country_code).where(Account.id == requester_account_id)
            )
            if country_code is None:
                return LeaderboardPage((), None)
            filters.append(LeaderboardEntry.country_code == country_code)

        order = (
            LeaderboardEntry.metric_value.desc(),
            LeaderboardEntry.tie_break_value.desc(),
            LeaderboardEntry.score_id.asc(),
        )
        rows = (
            await self._session.execute(_leaderboard_row_statement().where(*filters).order_by(*order).limit(limit))
        ).all()
        personal_row = (
            await self._session.execute(
                _leaderboard_row_statement()
                .where(*filters, LeaderboardEntry.account_id == requester_account_id)
                .limit(1)
            )
        ).one_or_none()
        score_ids = {row.score_id for row in rows}
        if personal_row is not None:
            score_ids.add(personal_row.score_id)
        hits = await self._score_hits(score_ids)
        scores = tuple(
            _leaderboard_view(row, rank=index + 1, ruleset=ruleset, hits=hits.get(row.score_id, {}))
            for index, row in enumerate(rows)
        )
        personal_best = None
        if personal_row is not None:
            personal_rank = next(
                (score.rank for score in scores if score.score_id == personal_row.score_id),
                None,
            )
            if personal_rank is None:
                personal_rank = 1 + int(
                    await self._session.scalar(
                        select(func.count())
                        .select_from(LeaderboardEntry)
                        .where(
                            *filters,
                            or_(
                                LeaderboardEntry.metric_value > personal_row.metric_value,
                                and_(
                                    LeaderboardEntry.metric_value == personal_row.metric_value,
                                    LeaderboardEntry.tie_break_value > personal_row.tie_break_value,
                                ),
                                and_(
                                    LeaderboardEntry.metric_value == personal_row.metric_value,
                                    LeaderboardEntry.tie_break_value == personal_row.tie_break_value,
                                    LeaderboardEntry.score_id < personal_row.score_id,
                                ),
                            ),
                        )
                    )
                    or 0
                )
            personal_best = _leaderboard_view(
                personal_row,
                rank=personal_rank,
                ruleset=ruleset,
                hits=hits.get(personal_row.score_id, {}),
            )
        return LeaderboardPage(scores, personal_best)

    async def get_account_stats(
        self,
        account_id: int,
        ruleset: Ruleset,
        variant: ScoreboardVariant,
    ) -> AccountStatsView:
        """Compose factual play totals with default-policy ranked statistics."""
        row = (
            await self._session.execute(
                select(
                    Scoreboard.id.label("scoreboard_id"),
                    RankingPolicy.id.label("policy_id"),
                    RankingPolicy.metric,
                    UserPlayStat.play_count,
                    UserPlayStat.total_score,
                    UserRankedStat.ranked_score,
                    UserRankedStat.accuracy,
                    UserRankedStat.performance,
                )
                .select_from(Scoreboard)
                .outerjoin(
                    UserPlayStat,
                    (UserPlayStat.scoreboard_id == Scoreboard.id) & (UserPlayStat.account_id == account_id),
                )
                .outerjoin(
                    RankingPolicy,
                    (RankingPolicy.scoreboard_id == Scoreboard.id)
                    & RankingPolicy.active.is_(True)
                    & RankingPolicy.is_default.is_(True),
                )
                .outerjoin(
                    UserRankedStat,
                    (UserRankedStat.policy_id == RankingPolicy.id) & (UserRankedStat.account_id == account_id),
                )
                .where(
                    Scoreboard.ruleset == DbRuleset(ruleset.value),
                    Scoreboard.variant == DbScoreboardVariant(variant.value),
                    Scoreboard.active.is_(True),
                )
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            return AccountStatsView(0, Decimal(0), 0, 0, 0)
        global_rank = 0
        if row.policy_id is not None and row.performance is not None:
            rank_column = UserRankedStat.performance
            rank_value = row.performance
            if rank_value > 0:
                higher = await self._session.scalar(
                    select(func.count())
                    .select_from(UserRankedStat)
                    .where(
                        UserRankedStat.policy_id == row.policy_id,
                        rank_column > rank_value,
                    )
                )
                global_rank = int(higher or 0) + 1
        return AccountStatsView(
            ranked_score=row.ranked_score or 0,
            accuracy=row.accuracy or Decimal(0),
            play_count=row.play_count or 0,
            total_score=row.total_score or 0,
            global_rank=global_rank,
            performance=int(row.performance) if row.performance is not None else 0,
        )

    async def get_beatmap_grades(
        self,
        account_id: int,
        beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapGradeView, ...]:
        """Return grades backed by active vanilla overall personal-best projections."""
        rows = (
            await self._session.execute(
                select(LeaderboardEntry.beatmap_id, Scoreboard.ruleset, Score.grade)
                .join(RankingPolicy, RankingPolicy.id == LeaderboardEntry.policy_id)
                .join(Scoreboard, Scoreboard.id == RankingPolicy.scoreboard_id)
                .join(Score, Score.id == LeaderboardEntry.score_id)
                .where(
                    LeaderboardEntry.account_id == account_id,
                    LeaderboardEntry.beatmap_id.in_(set(beatmap_ids)),
                    LeaderboardEntry.scope == "overall",
                    LeaderboardEntry.filter_mod_set_id.is_(None),
                    RankingPolicy.active.is_(True),
                    RankingPolicy.is_default.is_(True),
                    Scoreboard.variant == DbScoreboardVariant.VANILLA,
                )
                .order_by(LeaderboardEntry.beatmap_id, Scoreboard.id)
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

    async def _accepted_result_for_attempt(self, attempt_id: uuid.UUID) -> AcceptedScoreResult | None:
        row = (
            await self._session.execute(
                select(
                    Score.id,
                    Score.attempt_id,
                    Score.beatmap_id,
                    Score.beatmap_revision_id,
                    Score.scoreboard_id,
                    Score.mod_set_id,
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
            scoreboard_id=row.scoreboard_id,
            mod_set_id=row.mod_set_id,
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
        scoreboard: ScoreboardInfo,
        mod_set: ModSetInfo,
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
        expected_revision_id, expected_scoreboard_id, required_mod_set_id = dimensions
        if expected_revision_id != revision.revision_id or expected_scoreboard_id != scoreboard.scoreboard_id:
            raise MultiplayerContextRejected("multiplayer attempt dimensions do not match the score")
        if required_mod_set_id is not None and required_mod_set_id != mod_set.mod_set_id:
            raise MultiplayerContextRejected("multiplayer attempt mod set does not match the score")

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

    async def _submission_dimensions(self, attempt: MultiplayerAttempt) -> tuple[int, int, int | None] | None:
        if attempt.playlist_revision_id is not None:
            row = (
                await self._session.execute(
                    select(
                        PlaylistRevision.beatmap_revision_id,
                        PlaylistRevision.scoreboard_id,
                        PlaylistRevision.required_mod_set_id,
                    ).where(PlaylistRevision.id == attempt.playlist_revision_id)
                )
            ).one_or_none()
            if row is None:
                return None
            return row.beatmap_revision_id, row.scoreboard_id, row.required_mod_set_id
        if attempt.round_id is None:
            return None
        row = (
            await self._session.execute(
                select(
                    PlaylistRevision.beatmap_revision_id,
                    PlaylistRevision.scoreboard_id,
                    PlaylistRevision.required_mod_set_id,
                    RoundParticipant.mod_set_id.label("participant_mod_set_id"),
                    TournamentPoolItem.beatmap_revision_id.label("pool_revision_id"),
                    TournamentPoolItem.scoreboard_id.label("pool_scoreboard_id"),
                    TournamentPoolItem.mod_set_id.label("pool_mod_set_id"),
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
        if row is None:
            return None
        if row.beatmap_revision_id is not None:
            return (
                row.beatmap_revision_id,
                row.scoreboard_id,
                row.participant_mod_set_id or row.required_mod_set_id,
            )
        if row.pool_revision_id is None or row.pool_scoreboard_id is None:
            return None
        return row.pool_revision_id, row.pool_scoreboard_id, row.participant_mod_set_id or row.pool_mod_set_id


def _result_snapshot(result: AcceptedScoreResult) -> dict[str, object]:
    return {
        "attempt_id": str(result.attempt_id),
        "score_id": result.score_id,
        "beatmap_id": result.beatmap_id,
        "beatmap_revision_id": result.beatmap_revision_id,
        "scoreboard_id": result.scoreboard_id,
        "mod_set_id": result.mod_set_id,
        "outcome": result.outcome.value,
    }


def _leaderboard_row_statement() -> Select:
    return (
        select(
            LeaderboardEntry.score_id,
            LeaderboardEntry.account_id,
            LeaderboardEntry.metric_value,
            LeaderboardEntry.tie_break_value,
            Score.max_combo,
            Score.perfect,
            Score.ended_at,
            AccountName.display_name,
            ModSet.legacy_bits,
            Replay.score_id.label("replay_score_id"),
        )
        .join(Score, Score.id == LeaderboardEntry.score_id)
        .join(
            AccountName,
            and_(AccountName.account_id == LeaderboardEntry.account_id, AccountName.ended_at.is_(None)),
        )
        .join(ModSet, ModSet.id == Score.mod_set_id)
        .outerjoin(Replay, and_(Replay.score_id == Score.id, Replay.state == "ready"))
    )


def _leaderboard_view(
    row: object,
    *,
    rank: int,
    ruleset: Ruleset,
    hits: dict[str, int],
) -> LeaderboardScoreView:
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
        score_id=row.score_id,  # type: ignore[attr-defined]
        account_id=row.account_id,  # type: ignore[attr-defined]
        display_name=row.display_name,  # type: ignore[attr-defined]
        metric_value=row.metric_value,  # type: ignore[attr-defined]
        max_combo=row.max_combo,  # type: ignore[attr-defined]
        n50=n50,
        n100=n100,
        n300=n300,
        nmiss=hits.get("miss", 0),
        nkatu=nkatu,
        ngeki=ngeki,
        perfect=row.perfect,  # type: ignore[attr-defined]
        legacy_mod_bits=row.legacy_bits,  # type: ignore[attr-defined]
        rank=rank,
        ended_at=row.ended_at,  # type: ignore[attr-defined]
        has_replay=row.replay_score_id is not None,  # type: ignore[attr-defined]
    )


def _result_from_receipt(claim: ReceiptClaim) -> AcceptedScoreResult:
    value = claim.result_snapshot
    try:
        return AcceptedScoreResult(
            attempt_id=uuid.UUID(str(value["attempt_id"])),
            score_id=_receipt_integer(value["score_id"]),
            beatmap_id=_receipt_integer(value["beatmap_id"]),
            beatmap_revision_id=_receipt_integer(value["beatmap_revision_id"]),
            scoreboard_id=_receipt_integer(value["scoreboard_id"]),
            mod_set_id=_receipt_integer(value["mod_set_id"]),
            outcome=ScoreOutcome(str(value["outcome"])),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("score receipt contains an invalid result") from error


def _receipt_integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("score receipt identifier must be an integer")
    return value
