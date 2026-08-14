"""Query and mutate canonical beatmap content through SQLAlchemy."""

import uuid
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from sqlalchemy import and_, case, delete, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import Select

from perfcho.infra.db.enums import BeatmapStatus, BeatmapStatusEventSource, Ruleset
from perfcho.infra.db.models.content import (
    Beatmap,
    BeatmapRevision,
    Beatmapset,
    BeatmapsetFavourite,
    BeatmapsetStatusEvent,
    BeatmapsetSyncState,
    Comment,
    ContentSource,
    RatingVote,
)
from perfcho.infra.db.models.core import MediaAsset
from perfcho.infra.db.models.scoring import BeatmapDifficultyAttribute
from perfcho.infra.db.mods import canonical_mods_digest
from perfcho.modules.content.models import (
    BeatmapRevisionView,
    BeatmapsetStatusEventView,
    BeatmapsetStatusState,
    BeatmapsetView,
    CommentView,
    ContentSearch,
    ContentSearchPage,
    ContentSyncResult,
    FavouriteResult,
    RatingSummary,
    SyncedBeatmapFile,
    UpstreamBeatmapsetSnapshot,
    UpstreamBeatmapSnapshot,
)

_RevisionRow = tuple[
    BeatmapRevision,
    Beatmap,
    Beatmapset,
    Decimal,
    str,
    str,
    int,
]
_CurrentBeatmapMetadata = tuple[
    Ruleset,
    str,
    bytes | None,
    str | None,
    datetime | None,
    int | None,
    int | None,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    int | None,
    int | None,
    int | None,
    int | None,
    bool | None,
    bool | None,
]


class SqlAlchemyContentRepository:
    """Return scalar content projections and own no transactions."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind the caller-owned asynchronous session."""
        self._session = session

    async def lookup_md5(self, md5: bytes) -> BeatmapRevisionView | None:
        """Resolve any immutable revision by its globally unique MD5."""
        row = (await self._session.execute(_revision_statement().where(BeatmapRevision.md5 == md5))).one_or_none()
        return _revision_view(row._tuple() if row is not None else None)

    async def lookup_beatmap(self, beatmap_id: int, *, external: bool) -> BeatmapRevisionView | None:
        """Resolve the current revision by canonical or public beatmap ID."""
        selector = Beatmap.external_id == beatmap_id if external else Beatmap.id == beatmap_id
        row = (
            await self._session.execute(
                _revision_statement().where(selector, BeatmapRevision.is_current.is_(True)).limit(1)
            )
        ).one_or_none()
        return _revision_view(row._tuple() if row is not None else None)

    async def lookup_filename(self, file_name_key: str) -> BeatmapRevisionView | None:
        """Resolve the current revision by normalized filename."""
        row = (
            await self._session.execute(
                _revision_statement()
                .where(BeatmapRevision.file_name_key == file_name_key, BeatmapRevision.is_current.is_(True))
                .limit(1)
            )
        ).one_or_none()
        return _revision_view(row._tuple() if row is not None else None)

    async def batch_lookup(
        self,
        file_name_keys: tuple[str, ...],
        external_beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapRevisionView, ...]:
        """Resolve all song-select selectors with one bounded SQL statement."""
        if not file_name_keys and not external_beatmap_ids:
            return ()
        filters = []
        if file_name_keys:
            filters.append(BeatmapRevision.file_name_key.in_(set(file_name_keys)))
        if external_beatmap_ids:
            filters.append(Beatmap.external_id.in_(set(external_beatmap_ids)))
        rows = (
            await self._session.execute(
                _revision_statement()
                .where(BeatmapRevision.is_current.is_(True), or_(*filters))
                .order_by(Beatmap.external_id)
            )
        ).all()
        return tuple(_required_revision_view(row._tuple()) for row in rows)

    async def get_beatmapset(self, beatmapset_id: int, *, external: bool) -> BeatmapsetView | None:
        """Load a set and all current revisions in one query."""
        selector = Beatmapset.external_id == beatmapset_id if external else Beatmapset.id == beatmapset_id
        rows = (
            await self._session.execute(
                _revision_statement().where(selector, BeatmapRevision.is_current.is_(True)).order_by(Beatmap.id)
            )
        ).all()
        return _beatmapset_view(tuple(row._tuple() for row in rows))

    async def search(self, query: ContentSearch) -> ContentSearchPage:
        """Search current local beatmapsets and fetch their revisions in two queries."""
        statement = select(Beatmapset.id).where(Beatmapset.available.is_(True))
        if query.query.strip():
            pattern = f"%{query.query.strip()}%"
            statement = statement.where(
                or_(
                    Beatmapset.artist.ilike(pattern),
                    Beatmapset.title.ilike(pattern),
                    Beatmapset.creator_name.ilike(pattern),
                    Beatmapset.tags.ilike(pattern),
                )
            )
        if query.statuses:
            statement = statement.where(Beatmapset.status.in_(query.statuses))
        if query.ruleset is not None:
            statement = statement.where(
                select(literal(True))
                .where(Beatmap.beatmapset_id == Beatmapset.id, Beatmap.ruleset == query.ruleset)
                .exists()
            )
        identifiers = tuple(
            await self._session.scalars(
                statement.order_by(Beatmapset.id.desc()).offset(query.page * query.page_size).limit(query.page_size + 1)
            )
        )
        selected = identifiers[: query.page_size]
        if not selected:
            return ContentSearchPage((), False)
        rows = (
            await self._session.execute(
                _revision_statement()
                .where(Beatmapset.id.in_(selected), BeatmapRevision.is_current.is_(True))
                .order_by(Beatmapset.id.desc(), Beatmap.id)
            )
        ).all()
        grouped: dict[int, list[_RevisionRow]] = defaultdict(list)
        for row in rows:
            values = row._tuple()
            grouped[values[2].id].append(values)
        items = tuple(_required_beatmapset_view(grouped[identifier]) for identifier in selected if grouped[identifier])
        return ContentSearchPage(items, len(identifiers) > query.page_size)

    async def list_favourites(self, account_id: int) -> tuple[int, ...]:
        """Return public beatmapset IDs in stable order."""
        result = await self._session.scalars(
            select(Beatmapset.external_id)
            .join(BeatmapsetFavourite, BeatmapsetFavourite.beatmapset_id == Beatmapset.id)
            .where(BeatmapsetFavourite.account_id == account_id)
            .order_by(Beatmapset.external_id)
        )
        return tuple(result)

    async def set_favourite(self, account_id: int, beatmapset_id: int, favourited: bool) -> FavouriteResult:
        """Set a favourite using the public beatmapset ID."""
        internal_id = await self._session.scalar(
            select(Beatmapset.id).where(Beatmapset.external_id == beatmapset_id, Beatmapset.available.is_(True))
        )
        if internal_id is None:
            return FavouriteResult(account_id, beatmapset_id, False, False)
        if favourited:
            created = (
                await self._session.scalar(
                    insert(BeatmapsetFavourite)
                    .values(account_id=account_id, beatmapset_id=internal_id)
                    .on_conflict_do_nothing()
                    .returning(BeatmapsetFavourite.account_id)
                )
                is not None
            )
            return FavouriteResult(account_id, beatmapset_id, True, created)
        removed = (
            await self._session.scalar(
                delete(BeatmapsetFavourite)
                .where(
                    BeatmapsetFavourite.account_id == account_id,
                    BeatmapsetFavourite.beatmapset_id == internal_id,
                )
                .returning(BeatmapsetFavourite.account_id)
            )
            is not None
        )
        return FavouriteResult(account_id, beatmapset_id, False, removed)

    async def get_rating(self, beatmap_id: int, account_id: int | None) -> RatingSummary:
        """Return an aggregate and optional account rating by canonical logical beatmap ID."""
        return await self._rating_summary(beatmap_id, account_id)

    async def rate(self, account_id: int, beatmap_id: int, rating: int) -> RatingSummary:
        """Upsert one rating by canonical logical beatmap ID."""
        await self._session.execute(
            insert(RatingVote)
            .from_select(
                (RatingVote.account_id, RatingVote.beatmap_id, RatingVote.rating),
                select(literal(account_id), Beatmap.id, literal(rating)).where(
                    Beatmap.id == beatmap_id,
                    Beatmap.deleted_at.is_(None),
                ),
            )
            .on_conflict_do_update(
                index_elements=(RatingVote.account_id, RatingVote.beatmap_id),
                index_where=RatingVote.beatmap_id.is_not(None),
                set_={"rating": rating},
            )
        )
        return await self._rating_summary(beatmap_id, account_id)

    async def _rating_summary(self, beatmap_id: int, account_id: int | None) -> RatingSummary:
        account_rating = (
            func.max(RatingVote.rating).filter(RatingVote.account_id == account_id)
            if account_id is not None
            else literal(None)
        )
        row = (
            await self._session.execute(
                select(
                    func.avg(RatingVote.rating),
                    func.count(RatingVote.id),
                    account_rating,
                )
                .select_from(Beatmap)
                .outerjoin(RatingVote, RatingVote.beatmap_id == Beatmap.id)
                .where(Beatmap.id == beatmap_id, Beatmap.deleted_at.is_(None))
                .group_by(Beatmap.id)
            )
        ).one_or_none()
        if row is None:
            return RatingSummary(beatmap_id, None, 0, None)
        return RatingSummary(
            beatmap_id,
            Decimal(row[0]) if row[0] is not None else None,
            row[1],
            row[2],
        )

    async def list_comments(self, target: str, external_target_id: int) -> tuple[CommentView, ...]:
        """List visible comments for one external Stable target."""
        statement = select(Comment).where(Comment.moderation_state == "visible", Comment.deleted_at.is_(None))
        if target == "map":
            statement = statement.join(Beatmap, Beatmap.id == Comment.beatmap_id).where(
                Beatmap.external_id == external_target_id
            )
        elif target == "song":
            statement = statement.join(Beatmapset, Beatmapset.id == Comment.beatmapset_id).where(
                Beatmapset.external_id == external_target_id
            )
        else:
            statement = statement.where(Comment.score_id == external_target_id)
        comments = tuple(await self._session.scalars(statement.order_by(Comment.position_ms, Comment.id).limit(1000)))
        return tuple(_comment_view(comment, target) for comment in comments)

    async def create_comment(
        self,
        account_id: int,
        target: str,
        external_target_id: int,
        position_ms: int,
        body: str,
    ) -> CommentView:
        """Create one visible comment after resolving its external target."""
        values: dict[str, object] = {
            "author_account_id": account_id,
            "position_ms": position_ms,
            "body": body,
        }
        if target == "map":
            internal_id = await self._session.scalar(
                select(Beatmap.id).where(Beatmap.external_id == external_target_id)
            )
            values["beatmap_id"] = internal_id
        elif target == "song":
            internal_id = await self._session.scalar(
                select(Beatmapset.id).where(Beatmapset.external_id == external_target_id)
            )
            values["beatmapset_id"] = internal_id
        else:
            internal_id = external_target_id
            values["score_id"] = internal_id
        if internal_id is None:
            raise ValueError("comment target is unknown")
        created = Comment(**values)
        self._session.add(created)
        await self._session.flush()
        return _comment_view(created, target)

    async def synchronize_beatmapset(
        self,
        snapshot: UpstreamBeatmapsetSnapshot,
        files: tuple[SyncedBeatmapFile, ...],
        *,
        now: datetime,
        next_check_at: datetime,
    ) -> ContentSyncResult:
        """Publish verified objects as current revisions without deleting history."""
        snapshot_ids = {beatmap.external_beatmap_id for beatmap in snapshot.beatmaps}
        file_ids = {item.beatmap.external_beatmap_id for item in files}
        if snapshot_ids != file_ids or len(files) != len(snapshot.beatmaps):
            raise ValueError("synchronized files do not match the upstream snapshot")
        source_id = await self._session.scalar(
            select(ContentSource.id).where(ContentSource.code == snapshot.source_code)
        )
        if source_id is None:
            raise RuntimeError("content source is not bootstrapped")

        set_record = await self._session.scalar(
            select(Beatmapset)
            .where(Beatmapset.source_id == source_id, Beatmapset.external_id == snapshot.external_beatmapset_id)
            .with_for_update()
        )
        if set_record is None:
            set_record = await self._session.scalar(
                insert(Beatmapset)
                .values(
                    source_id=source_id,
                    external_id=snapshot.external_beatmapset_id,
                    creator_external_id=snapshot.creator_external_id,
                    creator_name=snapshot.creator_name,
                    artist=snapshot.artist,
                    artist_unicode=snapshot.artist_unicode,
                    title=snapshot.title,
                    title_unicode=snapshot.title_unicode,
                    source_text=snapshot.source_text,
                    tags=snapshot.tags,
                    genre_id=snapshot.genre_id,
                    language_id=snapshot.language_id,
                    description=snapshot.description,
                    status=BeatmapStatus(snapshot.status),
                    source_status=BeatmapStatus(snapshot.status),
                    submitted_at=snapshot.submitted_at,
                    ranked_at=snapshot.ranked_at,
                    last_source_update_at=snapshot.last_updated_at,
                    available=snapshot.available,
                    nsfw=snapshot.nsfw,
                )
                .on_conflict_do_nothing(index_elements=(Beatmapset.source_id, Beatmapset.external_id))
                .returning(Beatmapset)
            )
            previous_source_update = None
            if set_record is None:
                set_record = await self._session.scalar(
                    select(Beatmapset)
                    .where(
                        Beatmapset.source_id == source_id,
                        Beatmapset.external_id == snapshot.external_beatmapset_id,
                    )
                    .with_for_update()
                )
                if set_record is None:
                    raise RuntimeError("database did not resolve the synchronized beatmapset")
                previous_source_update = set_record.last_source_update_at
            beatmapset_id = set_record.id
        else:
            beatmapset_id = set_record.id
            previous_source_update = set_record.last_source_update_at

        stale_snapshot = previous_source_update is not None and previous_source_update > snapshot.last_updated_at
        equal_version_conflict = (
            previous_source_update == snapshot.last_updated_at
            and not await self._snapshot_extends_current(
                set_record,
                beatmapset_id,
                snapshot,
            )
        )
        if stale_snapshot or equal_version_conflict:
            return ContentSyncResult(
                beatmapset_id=beatmapset_id,
                external_beatmapset_id=snapshot.external_beatmapset_id,
                created_revision_count=0,
                unchanged_revision_count=0,
                removed_beatmap_count=0,
                published=False,
            )

        snapshot_status = BeatmapStatus(snapshot.status)
        status_overridden = set_record.status != set_record.source_status
        beatmapset_values: dict[str, object] = {
            "creator_external_id": snapshot.creator_external_id,
            "creator_name": snapshot.creator_name,
            "artist": snapshot.artist,
            "artist_unicode": snapshot.artist_unicode,
            "title": snapshot.title,
            "title_unicode": snapshot.title_unicode,
            "source_text": snapshot.source_text,
            "tags": snapshot.tags,
            "genre_id": snapshot.genre_id,
            "language_id": snapshot.language_id,
            "description": snapshot.description,
            "source_status": snapshot_status,
            "submitted_at": snapshot.submitted_at,
            "ranked_at": snapshot.ranked_at,
            "last_source_update_at": snapshot.last_updated_at,
            "available": snapshot.available,
            "nsfw": snapshot.nsfw,
        }
        if not status_overridden:
            beatmapset_values["status"] = snapshot_status
        await self._session.execute(update(Beatmapset).where(Beatmapset.id == beatmapset_id).values(beatmapset_values))

        if set_record.status != snapshot_status:
            await self._session.execute(
                insert(BeatmapsetStatusEvent).values(
                    beatmapset_id=beatmapset_id,
                    previous_status=set_record.status,
                    status=snapshot_status,
                    source=BeatmapStatusEventSource.UPSTREAM_SYNC,
                    actor_account_id=None,
                    effective_at=now,
                )
            )

        beatmap_rows = (
            await self._session.execute(
                insert(Beatmap)
                .values(
                    [
                        {
                            "beatmapset_id": beatmapset_id,
                            "source_id": source_id,
                            "external_id": item.beatmap.external_beatmap_id,
                            "ruleset": Ruleset(item.beatmap.ruleset),
                            "difficulty_name": item.beatmap.difficulty_name,
                            "deleted_at": None,
                        }
                        for item in files
                    ]
                )
                .on_conflict_do_update(
                    index_elements=(Beatmap.source_id, Beatmap.external_id),
                    set_={
                        "beatmapset_id": beatmapset_id,
                        "ruleset": insert(Beatmap).excluded.ruleset,
                        "difficulty_name": insert(Beatmap).excluded.difficulty_name,
                        "deleted_at": None,
                    },
                )
                .returning(Beatmap.id, Beatmap.external_id)
            )
        ).all()
        beatmap_ids = {row.external_id: row.id for row in beatmap_rows}
        if len(beatmap_ids) != len(files):
            raise RuntimeError("database did not return every synchronized beatmap")

        asset_values = []
        requested_digests = set()
        for item in files:
            digest = item.stored_object.sha256
            assert digest is not None
            requested_digests.add(digest)
            asset_values.append(
                {
                    "id": item.asset_id,
                    "storage_key": item.stored_object.storage_key,
                    "sha256": digest,
                    "media_type": item.stored_object.media_type,
                    "size_bytes": item.stored_object.size_bytes,
                }
            )
        await self._session.execute(insert(MediaAsset).values(asset_values).on_conflict_do_nothing())
        asset_rows = (
            await self._session.execute(
                select(MediaAsset.id, MediaAsset.sha256).where(MediaAsset.sha256.in_(requested_digests))
            )
        ).all()
        assets_by_digest = {row.sha256: row.id for row in asset_rows}
        if assets_by_digest.keys() != requested_digests:
            raise RuntimeError("media asset conflict did not resolve by digest")

        revision_rows = (
            await self._session.execute(
                select(
                    BeatmapRevision.id,
                    BeatmapRevision.beatmap_id,
                    BeatmapRevision.sha256,
                    BeatmapRevision.is_current,
                )
                .where(BeatmapRevision.beatmap_id.in_(beatmap_ids.values()))
                .with_for_update()
            )
        ).all()
        revisions_by_digest = {(row.beatmap_id, row.sha256): row for row in revision_rows}
        current_by_beatmap = {row.beatmap_id: row for row in revision_rows if row.is_current}
        changed_beatmap_ids = []
        restored_assets: dict[int, uuid.UUID] = {}
        new_revisions = []
        unchanged_revision_count = 0

        for item in files:
            beatmap = item.beatmap
            beatmap_id = beatmap_ids[beatmap.external_beatmap_id]
            digest = item.stored_object.sha256
            assert digest is not None
            asset_id = assets_by_digest[digest]
            current = current_by_beatmap.get(beatmap_id)
            if current is not None and current.sha256 == digest:
                unchanged_revision_count += 1
                continue

            changed_beatmap_ids.append(beatmap_id)
            historical = revisions_by_digest.get((beatmap_id, digest))
            if historical is not None:
                restored_assets[historical.id] = asset_id
                continue
            new_revisions.append(
                {
                    "beatmap_id": beatmap_id,
                    "file_asset_id": asset_id,
                    "md5": beatmap.md5,
                    "sha256": digest,
                    "file_name": beatmap.file_name,
                    "file_name_key": func.lower(beatmap.file_name),
                    "source_updated_at": beatmap.source_updated_at,
                    "total_length_ms": beatmap.total_length_ms,
                    "drain_length_ms": beatmap.drain_length_ms,
                    "bpm": beatmap.bpm,
                    "circle_size": beatmap.circle_size,
                    "overall_difficulty": beatmap.overall_difficulty,
                    "approach_rate": beatmap.approach_rate,
                    "health_drain": beatmap.health_drain,
                    "object_count": beatmap.object_count,
                    "circle_count": beatmap.circle_count,
                    "slider_count": beatmap.slider_count,
                    "spinner_count": beatmap.spinner_count,
                    "max_combo": beatmap.max_combo,
                    "has_storyboard": beatmap.has_storyboard,
                    "has_video": beatmap.has_video,
                    "is_current": True,
                }
            )

        if changed_beatmap_ids:
            await self._session.execute(
                update(BeatmapRevision)
                .where(
                    BeatmapRevision.beatmap_id.in_(changed_beatmap_ids),
                    BeatmapRevision.is_current.is_(True),
                )
                .values(is_current=False)
            )
        if restored_assets:
            await self._session.execute(
                update(BeatmapRevision)
                .where(BeatmapRevision.id.in_(restored_assets))
                .values(
                    is_current=True,
                    file_asset_id=case(
                        {
                            revision_id: literal(asset_id, type_=MediaAsset.id.type)
                            for revision_id, asset_id in restored_assets.items()
                        },
                        value=BeatmapRevision.id,
                    ),
                )
            )
        if new_revisions:
            await self._session.execute(insert(BeatmapRevision).values(new_revisions))
        created_revision_count = len(changed_beatmap_ids)

        removed_ids = tuple(
            await self._session.scalars(
                update(Beatmap)
                .where(
                    Beatmap.source_id == source_id,
                    Beatmap.beatmapset_id == beatmapset_id,
                    Beatmap.external_id.not_in(snapshot_ids),
                    Beatmap.deleted_at.is_(None),
                )
                .values(deleted_at=now)
                .returning(Beatmap.id)
            )
        )
        await self._session.execute(
            insert(BeatmapsetSyncState)
            .values(
                beatmapset_id=beatmapset_id,
                last_checked_at=now,
                next_check_at=next_check_at,
                error_count=0,
            )
            .on_conflict_do_update(
                index_elements=(BeatmapsetSyncState.beatmapset_id,),
                set_={
                    "last_checked_at": now,
                    "next_check_at": next_check_at,
                    "error_count": 0,
                },
            )
        )
        return ContentSyncResult(
            beatmapset_id=beatmapset_id,
            external_beatmapset_id=snapshot.external_beatmapset_id,
            created_revision_count=created_revision_count,
            unchanged_revision_count=unchanged_revision_count,
            removed_beatmap_count=len(removed_ids),
        )

    async def claim_beatmapset_refresh(
        self,
        external_beatmapset_id: int,
        *,
        now: datetime,
        lease_until: datetime,
    ) -> bool:
        """Atomically move one due official beatmapset's next check behind a short lease."""
        beatmapset_id = await self._official_beatmapset_id(external_beatmapset_id)
        if beatmapset_id is None:
            return False
        inserted = await self._session.scalar(
            insert(BeatmapsetSyncState)
            .values(
                beatmapset_id=beatmapset_id,
                next_check_at=lease_until,
                error_count=0,
            )
            .on_conflict_do_nothing(index_elements=(BeatmapsetSyncState.beatmapset_id,))
            .returning(BeatmapsetSyncState.beatmapset_id)
        )
        if inserted is not None:
            return True
        claimed = await self._session.scalar(
            update(BeatmapsetSyncState)
            .where(
                BeatmapsetSyncState.beatmapset_id == beatmapset_id,
                or_(BeatmapsetSyncState.next_check_at.is_(None), BeatmapsetSyncState.next_check_at <= now),
            )
            .values(next_check_at=lease_until)
            .returning(BeatmapsetSyncState.beatmapset_id)
        )
        return claimed is not None

    async def record_beatmapset_refresh_failure(
        self,
        external_beatmapset_id: int,
        *,
        expected_lease_until: datetime,
        checked_at: datetime,
        next_check_at: datetime,
        error: str,
    ) -> None:
        """Release a failed refresh lease with bounded diagnostic state."""
        beatmapset_id = await self._official_beatmapset_id(external_beatmapset_id)
        if beatmapset_id is None:
            return
        await self._session.execute(
            update(BeatmapsetSyncState)
            .where(
                BeatmapsetSyncState.beatmapset_id == beatmapset_id,
                BeatmapsetSyncState.next_check_at == expected_lease_until,
            )
            .values(
                last_checked_at=checked_at,
                next_check_at=next_check_at,
                error_count=BeatmapsetSyncState.error_count + 1,
            )
        )

    async def _official_beatmapset_id(self, external_beatmapset_id: int) -> int | None:
        return await self._session.scalar(
            select(Beatmapset.id)
            .join(ContentSource, ContentSource.id == Beatmapset.source_id)
            .where(ContentSource.code == "osu", Beatmapset.external_id == external_beatmapset_id)
        )

    async def get_status_state(self, external_beatmapset_id: int, *, for_update: bool) -> BeatmapsetStatusState | None:
        """Return one beatmapset's authoritative and upstream status, optionally locking it."""
        statement = select(Beatmapset).where(Beatmapset.external_id == external_beatmapset_id)
        if for_update:
            statement = statement.with_for_update()
        beatmapset = await self._session.scalar(statement)
        if beatmapset is None:
            return None
        return BeatmapsetStatusState(
            beatmapset_id=beatmapset.id,
            external_beatmapset_id=beatmapset.external_id,
            status=beatmapset.status.value,
            source_status=beatmapset.source_status.value,
        )

    async def apply_status_transition(
        self,
        beatmapset_id: int,
        previous_status: BeatmapStatus,
        target_status: BeatmapStatus,
        *,
        source: BeatmapStatusEventSource,
        actor_account_id: int | None,
        reason: str | None,
        effective_at: datetime,
    ) -> None:
        """Persist one status transition and its event inside the caller-owned transaction."""
        await self._session.execute(
            update(Beatmapset).where(Beatmapset.id == beatmapset_id).values(status=target_status)
        )
        await self._session.execute(
            insert(BeatmapsetStatusEvent).values(
                beatmapset_id=beatmapset_id,
                previous_status=previous_status,
                status=target_status,
                source=source,
                actor_account_id=actor_account_id,
                reason=reason,
                effective_at=effective_at,
            )
        )

    async def revert_status(
        self, beatmapset_id: int, *, source: BeatmapStatusEventSource, effective_at: datetime
    ) -> None:
        """Restore the authoritative status to the upstream source status."""
        state = await self._session.scalar(
            select(Beatmapset.status, Beatmapset.source_status).where(Beatmapset.id == beatmapset_id).with_for_update()
        )
        if state is None:
            raise RuntimeError("beatmapset status cannot be resolved")
        current_status, source_status = state
        if current_status == source_status:
            return
        await self._session.execute(
            update(Beatmapset).where(Beatmapset.id == beatmapset_id).values(status=source_status)
        )
        await self._session.execute(
            insert(BeatmapsetStatusEvent).values(
                beatmapset_id=beatmapset_id,
                previous_status=current_status,
                status=source_status,
                source=source,
                actor_account_id=None,
                effective_at=effective_at,
            )
        )

    async def list_status_events(self, external_beatmapset_id: int) -> tuple[BeatmapsetStatusEventView, ...]:
        """List a beatmapset's status transitions in chronological order."""
        rows = (
            await self._session.execute(
                select(BeatmapsetStatusEvent)
                .join(Beatmapset, Beatmapset.id == BeatmapsetStatusEvent.beatmapset_id)
                .where(Beatmapset.external_id == external_beatmapset_id)
                .order_by(BeatmapsetStatusEvent.effective_at, BeatmapsetStatusEvent.id)
            )
        ).scalars()
        return tuple(_status_event_view(event) for event in rows)

    async def _snapshot_extends_current(
        self,
        beatmapset: Beatmapset | None,
        beatmapset_id: int,
        snapshot: UpstreamBeatmapsetSnapshot,
    ) -> bool:
        if beatmapset is None or _beatmapset_metadata(beatmapset) != _snapshot_beatmapset_metadata(snapshot):
            return False
        rows = (
            await self._session.execute(
                select(Beatmap, BeatmapRevision)
                .outerjoin(
                    BeatmapRevision,
                    and_(
                        BeatmapRevision.beatmap_id == Beatmap.id,
                        BeatmapRevision.is_current.is_(True),
                    ),
                )
                .where(Beatmap.beatmapset_id == beatmapset_id)
            )
        ).all()
        current: dict[int, tuple[object, ...] | None] = {}
        for row in rows:
            beatmap, revision_row = row._tuple()
            revision: BeatmapRevision | None = revision_row
            metadata: _CurrentBeatmapMetadata = (
                beatmap.ruleset,
                beatmap.difficulty_name,
                revision.md5 if revision is not None else None,
                revision.file_name if revision is not None else None,
                revision.source_updated_at if revision is not None else None,
                revision.total_length_ms if revision is not None else None,
                revision.drain_length_ms if revision is not None else None,
                revision.bpm if revision is not None else None,
                revision.circle_size if revision is not None else None,
                revision.overall_difficulty if revision is not None else None,
                revision.approach_rate if revision is not None else None,
                revision.health_drain if revision is not None else None,
                revision.circle_count if revision is not None else None,
                revision.slider_count if revision is not None else None,
                revision.spinner_count if revision is not None else None,
                revision.max_combo if revision is not None else None,
                revision.has_storyboard if revision is not None else None,
                revision.has_video if revision is not None else None,
            )
            current[beatmap.external_id] = (
                None if beatmap.deleted_at is not None else _current_beatmap_metadata(metadata)
            )
        return _snapshot_extends_current_revision_set(current, snapshot)


def _revision_statement() -> Select[*_RevisionRow]:
    no_mod_digest = canonical_mods_digest(())
    no_mod_stars = (
        select(func.max(BeatmapDifficultyAttribute.star_rating))
        .where(
            BeatmapDifficultyAttribute.beatmap_revision_id == BeatmapRevision.id,
            BeatmapDifficultyAttribute.ruleset == Beatmap.ruleset,
            BeatmapDifficultyAttribute.mods_digest == no_mod_digest,
        )
        .correlate(BeatmapRevision, Beatmap)
        .scalar_subquery()
    )
    return (
        select(
            BeatmapRevision,
            Beatmap,
            Beatmapset,
            no_mod_stars.label("star_rating"),
            MediaAsset.storage_key.label("file_storage_key"),
            MediaAsset.media_type.label("file_media_type"),
            MediaAsset.size_bytes.label("file_size_bytes"),
        )
        .select_from(BeatmapRevision)
        .join(Beatmap, Beatmap.id == BeatmapRevision.beatmap_id)
        .join(Beatmapset, Beatmapset.id == Beatmap.beatmapset_id)
        .outerjoin(MediaAsset, MediaAsset.id == BeatmapRevision.file_asset_id)
    )


def _snapshot_extends_current_revision_set(
    current: dict[int, tuple[object, ...] | None],
    snapshot: UpstreamBeatmapsetSnapshot,
) -> bool:
    incoming = {beatmap.external_beatmap_id: _snapshot_beatmap_metadata(beatmap) for beatmap in snapshot.beatmaps}
    return all(
        external_id not in incoming if metadata is None else incoming.get(external_id) == metadata
        for external_id, metadata in current.items()
    )


def _status_event_view(event: BeatmapsetStatusEvent) -> BeatmapsetStatusEventView:
    return BeatmapsetStatusEventView(
        beatmapset_id=event.beatmapset_id,
        previous_status=event.previous_status.value if event.previous_status is not None else None,
        status=event.status.value,
        source=event.source.value,
        actor_account_id=event.actor_account_id,
        reason=event.reason,
        effective_at=event.effective_at,
    )


def _comment_view(comment: Comment, target: str) -> CommentView:
    return CommentView(
        comment_id=comment.id,
        author_account_id=comment.author_account_id,
        target=target,
        position_ms=comment.position_ms or 0,
        body=comment.body,
        color=comment.color,
        created_at=comment.created_at,
    )


def _beatmapset_metadata(beatmapset: Beatmapset) -> tuple[object, ...]:
    return (
        beatmapset.creator_external_id,
        beatmapset.creator_name,
        beatmapset.artist,
        beatmapset.artist_unicode,
        beatmapset.title,
        beatmapset.title_unicode,
        beatmapset.source_text,
        beatmapset.tags,
        beatmapset.genre_id,
        beatmapset.language_id,
        beatmapset.description,
        beatmapset.source_status.value,
        beatmapset.submitted_at,
        beatmapset.ranked_at,
        beatmapset.available,
        beatmapset.nsfw,
    )


def _snapshot_beatmapset_metadata(snapshot: UpstreamBeatmapsetSnapshot) -> tuple[object, ...]:
    return (
        snapshot.creator_external_id,
        snapshot.creator_name,
        snapshot.artist,
        snapshot.artist_unicode,
        snapshot.title,
        snapshot.title_unicode,
        snapshot.source_text,
        snapshot.tags,
        snapshot.genre_id,
        snapshot.language_id,
        snapshot.description,
        snapshot.status,
        snapshot.submitted_at,
        snapshot.ranked_at,
        snapshot.available,
        snapshot.nsfw,
    )


def _current_beatmap_metadata(row: _CurrentBeatmapMetadata) -> tuple[object, ...]:
    (
        ruleset,
        difficulty_name,
        md5,
        file_name,
        source_updated_at,
        total_length_ms,
        drain_length_ms,
        bpm,
        circle_size,
        overall_difficulty,
        approach_rate,
        health_drain,
        circle_count,
        slider_count,
        spinner_count,
        max_combo,
        has_storyboard,
        has_video,
    ) = row
    return (
        ruleset.value,
        difficulty_name,
        md5,
        file_name,
        source_updated_at,
        total_length_ms,
        drain_length_ms,
        bpm,
        circle_size,
        overall_difficulty,
        approach_rate,
        health_drain,
        circle_count,
        slider_count,
        spinner_count,
        max_combo,
        has_storyboard,
        has_video,
    )


def _snapshot_beatmap_metadata(beatmap: UpstreamBeatmapSnapshot) -> tuple[object, ...]:
    return (
        beatmap.ruleset,
        beatmap.difficulty_name,
        beatmap.md5,
        beatmap.file_name,
        beatmap.source_updated_at,
        beatmap.total_length_ms,
        beatmap.drain_length_ms,
        beatmap.bpm,
        beatmap.circle_size,
        beatmap.overall_difficulty,
        beatmap.approach_rate,
        beatmap.health_drain,
        beatmap.circle_count,
        beatmap.slider_count,
        beatmap.spinner_count,
        beatmap.max_combo,
        beatmap.has_storyboard,
        beatmap.has_video,
    )


def _revision_view(row: _RevisionRow | None) -> BeatmapRevisionView | None:
    return None if row is None else _required_revision_view(row)


def _required_revision_view(row: _RevisionRow) -> BeatmapRevisionView:
    (
        revision,
        beatmap,
        beatmapset,
        star_rating,
        file_storage_key,
        file_media_type,
        file_size_bytes,
    ) = row
    return BeatmapRevisionView(
        beatmap_id=beatmap.id,
        external_beatmap_id=beatmap.external_id,
        beatmapset_id=beatmapset.id,
        external_beatmapset_id=beatmapset.external_id,
        revision_id=revision.id,
        md5=revision.md5,
        sha256=revision.sha256,
        file_name=revision.file_name,
        artist=beatmapset.artist,
        title=beatmapset.title,
        creator=beatmapset.creator_name,
        difficulty_name=beatmap.difficulty_name,
        ruleset=beatmap.ruleset.value,
        status=beatmapset.status.value,
        source_updated_at=revision.source_updated_at,
        total_length_ms=revision.total_length_ms,
        drain_length_ms=revision.drain_length_ms,
        bpm=revision.bpm,
        circle_size=revision.circle_size,
        overall_difficulty=revision.overall_difficulty,
        approach_rate=revision.approach_rate,
        health_drain=revision.health_drain,
        object_count=revision.object_count,
        max_combo=revision.max_combo,
        star_rating=star_rating,
        has_video=revision.has_video,
        is_current=revision.is_current,
        file_storage_key=file_storage_key,
        file_media_type=file_media_type,
        file_size_bytes=file_size_bytes,
    )


def _beatmapset_view(rows: Sequence[_RevisionRow]) -> BeatmapsetView | None:
    return None if not rows else _required_beatmapset_view(rows)


def _required_beatmapset_view(rows: Sequence[_RevisionRow]) -> BeatmapsetView:
    _, _, beatmapset, _, _, _, _ = rows[0]
    beatmaps = tuple(_required_revision_view(row) for row in rows)
    return BeatmapsetView(
        beatmapset_id=beatmapset.id,
        external_beatmapset_id=beatmapset.external_id,
        artist=beatmapset.artist,
        title=beatmapset.title,
        creator=beatmapset.creator_name,
        status=beatmapset.status.value,
        last_updated_at=beatmapset.last_source_update_at,
        available=beatmapset.available,
        has_video=any(item.has_video for item in beatmaps),
        beatmaps=beatmaps,
    )
