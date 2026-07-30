"""Query and mutate canonical beatmap content through SQLAlchemy."""

import uuid
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import delete, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import Select

from perfcho.infra.db.enums import BeatmapStatus, Ruleset
from perfcho.infra.db.models.content import (
    Beatmap,
    BeatmapRevision,
    Beatmapset,
    BeatmapsetFavourite,
    BeatmapStatusEvent,
    ContentSource,
    ContentSyncState,
    RatingVote,
)
from perfcho.infra.db.models.core import MediaAsset
from perfcho.infra.db.models.scoring import BeatmapDifficultyAttribute, ModSet, Scoreboard
from perfcho.modules.content.models import (
    BeatmapRevisionView,
    BeatmapsetView,
    ContentSearch,
    ContentSearchPage,
    ContentSyncResult,
    FavouriteResult,
    RatingSummary,
    SyncedBeatmapFile,
    UpstreamBeatmapsetSnapshot,
)


class SqlAlchemyContentRepository:
    """Return scalar content projections and own no transactions."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind the caller-owned asynchronous session."""
        self._session = session

    async def lookup_md5(self, md5: bytes) -> BeatmapRevisionView | None:
        """Resolve any immutable revision by its globally unique MD5."""
        row = (await self._session.execute(_revision_statement().where(BeatmapRevision.md5 == md5))).one_or_none()
        return _revision_view(row)

    async def lookup_beatmap(self, beatmap_id: int, *, external: bool) -> BeatmapRevisionView | None:
        """Resolve the current revision by canonical or public beatmap ID."""
        selector = Beatmap.external_id == beatmap_id if external else Beatmap.id == beatmap_id
        row = (
            await self._session.execute(
                _revision_statement().where(selector, BeatmapRevision.is_current.is_(True)).limit(1)
            )
        ).one_or_none()
        return _revision_view(row)

    async def lookup_filename(self, file_name_key: str) -> BeatmapRevisionView | None:
        """Resolve the current revision by normalized filename."""
        row = (
            await self._session.execute(
                _revision_statement()
                .where(BeatmapRevision.file_name_key == file_name_key, BeatmapRevision.is_current.is_(True))
                .limit(1)
            )
        ).one_or_none()
        return _revision_view(row)

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
        return tuple(_required_revision_view(row) for row in rows)

    async def get_beatmapset(self, beatmapset_id: int, *, external: bool) -> BeatmapsetView | None:
        """Load a set and all current revisions in one query."""
        selector = Beatmapset.external_id == beatmapset_id if external else Beatmapset.id == beatmapset_id
        rows = (
            await self._session.execute(
                _revision_statement().where(selector, BeatmapRevision.is_current.is_(True)).order_by(Beatmap.id)
            )
        ).all()
        return _beatmapset_view(rows)

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
        grouped: dict[int, list[object]] = defaultdict(list)
        for row in rows:
            grouped[row.beatmapset_id].append(row)
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
        exists = await self._session.scalar(
            select(Beatmap.id).where(Beatmap.id == beatmap_id, Beatmap.deleted_at.is_(None))
        )
        if exists is None:
            return RatingSummary(beatmap_id, None, 0, None)
        aggregate = (
            await self._session.execute(
                select(func.avg(RatingVote.rating), func.count(RatingVote.id)).where(
                    RatingVote.beatmap_id == beatmap_id
                )
            )
        ).one()
        account_rating = None
        if account_id is not None:
            account_rating = await self._session.scalar(
                select(RatingVote.rating).where(
                    RatingVote.account_id == account_id,
                    RatingVote.beatmap_id == beatmap_id,
                )
            )
        return RatingSummary(
            beatmap_id,
            Decimal(aggregate[0]) if aggregate[0] is not None else None,
            aggregate[1],
            account_rating,
        )

    async def rate(self, account_id: int, beatmap_id: int, rating: int) -> RatingSummary:
        """Upsert one rating by canonical logical beatmap ID."""
        exists = await self._session.scalar(
            select(Beatmap.id).where(Beatmap.id == beatmap_id, Beatmap.deleted_at.is_(None))
        )
        if exists is None:
            return RatingSummary(beatmap_id, None, 0, None)
        await self._session.execute(
            insert(RatingVote)
            .values(account_id=account_id, beatmap_id=beatmap_id, rating=rating)
            .on_conflict_do_update(
                index_elements=(RatingVote.account_id, RatingVote.beatmap_id),
                index_where=RatingVote.beatmap_id.is_not(None),
                set_={"rating": rating},
            )
        )
        return await self.get_rating(beatmap_id, account_id)

    async def synchronize_beatmapset(
        self,
        snapshot: UpstreamBeatmapsetSnapshot,
        files: tuple[SyncedBeatmapFile, ...],
        *,
        now: datetime,
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

        beatmapset_id = await self._session.scalar(
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
                submitted_at=snapshot.submitted_at,
                ranked_at=snapshot.ranked_at,
                last_source_update_at=snapshot.last_updated_at,
                available=snapshot.available,
                nsfw=snapshot.nsfw,
            )
            .on_conflict_do_update(
                index_elements=(Beatmapset.source_id, Beatmapset.external_id),
                set_={
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
                    "status": BeatmapStatus(snapshot.status),
                    "submitted_at": snapshot.submitted_at,
                    "ranked_at": snapshot.ranked_at,
                    "last_source_update_at": snapshot.last_updated_at,
                    "available": snapshot.available,
                    "nsfw": snapshot.nsfw,
                },
            )
            .returning(Beatmapset.id)
        )
        if beatmapset_id is None:
            raise RuntimeError("database did not return the synchronized beatmapset")

        existing_rows = (
            await self._session.execute(
                select(Beatmap.id, Beatmap.external_id, Beatmap.status)
                .where(Beatmap.source_id == source_id, Beatmap.beatmapset_id == beatmapset_id)
                .with_for_update()
            )
        ).all()
        existing = {row.external_id: row for row in existing_rows}
        created_revision_count = 0
        unchanged_revision_count = 0

        for item in files:
            beatmap = item.beatmap
            previous = existing.get(beatmap.external_beatmap_id)
            beatmap_id = await self._session.scalar(
                insert(Beatmap)
                .values(
                    beatmapset_id=beatmapset_id,
                    source_id=source_id,
                    external_id=beatmap.external_beatmap_id,
                    ruleset=Ruleset(beatmap.ruleset),
                    difficulty_name=beatmap.difficulty_name,
                    status=BeatmapStatus(beatmap.status),
                    deleted_at=None,
                )
                .on_conflict_do_update(
                    index_elements=(Beatmap.source_id, Beatmap.external_id),
                    set_={
                        "beatmapset_id": beatmapset_id,
                        "ruleset": Ruleset(beatmap.ruleset),
                        "difficulty_name": beatmap.difficulty_name,
                        "status": BeatmapStatus(beatmap.status),
                        "deleted_at": None,
                    },
                )
                .returning(Beatmap.id)
            )
            if beatmap_id is None:
                raise RuntimeError("database did not return the synchronized beatmap")
            if previous is not None and previous.status.value != beatmap.status:
                self._session.add(
                    BeatmapStatusEvent(
                        beatmap_id=beatmap_id,
                        previous_status=previous.status,
                        status=BeatmapStatus(beatmap.status),
                        source="upstream_sync",
                        effective_at=now,
                    )
                )

            asset_id = await self._ensure_media_asset(item)
            current = (
                await self._session.execute(
                    select(BeatmapRevision.id, BeatmapRevision.sha256)
                    .where(BeatmapRevision.beatmap_id == beatmap_id, BeatmapRevision.is_current.is_(True))
                    .with_for_update()
                )
            ).one_or_none()
            if current is not None and current.sha256 == item.stored_object.sha256:
                unchanged_revision_count += 1
                continue

            await self._session.execute(
                update(BeatmapRevision)
                .where(BeatmapRevision.beatmap_id == beatmap_id, BeatmapRevision.is_current.is_(True))
                .values(is_current=False)
            )
            historical_revision_id = await self._session.scalar(
                select(BeatmapRevision.id).where(
                    BeatmapRevision.beatmap_id == beatmap_id,
                    BeatmapRevision.sha256 == item.stored_object.sha256,
                )
            )
            if historical_revision_id is not None:
                await self._session.execute(
                    update(BeatmapRevision)
                    .where(BeatmapRevision.id == historical_revision_id)
                    .values(is_current=True, file_asset_id=asset_id)
                )
            else:
                await self._session.execute(
                    insert(BeatmapRevision).values(
                        beatmap_id=beatmap_id,
                        file_asset_id=asset_id,
                        md5=beatmap.md5,
                        sha256=item.stored_object.sha256,
                        file_name=beatmap.file_name,
                        file_name_key=func.lower(beatmap.file_name),
                        source_updated_at=beatmap.source_updated_at,
                        total_length_ms=beatmap.total_length_ms,
                        drain_length_ms=beatmap.drain_length_ms,
                        bpm=beatmap.bpm,
                        circle_size=beatmap.circle_size,
                        overall_difficulty=beatmap.overall_difficulty,
                        approach_rate=beatmap.approach_rate,
                        health_drain=beatmap.health_drain,
                        object_count=beatmap.object_count,
                        circle_count=beatmap.circle_count,
                        slider_count=beatmap.slider_count,
                        spinner_count=beatmap.spinner_count,
                        max_combo=beatmap.max_combo,
                        has_storyboard=beatmap.has_storyboard,
                        has_video=beatmap.has_video,
                        is_current=True,
                    )
                )
            created_revision_count += 1

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
        unchanged_sync = int(created_revision_count == 0 and not removed_ids)
        await self._session.execute(
            insert(ContentSyncState)
            .values(
                beatmapset_id=beatmapset_id,
                etag=snapshot.etag,
                last_modified=snapshot.last_modified,
                last_checked_at=now,
                next_check_at=now + timedelta(hours=24),
                unchanged_count=unchanged_sync,
                error_count=0,
                last_error=None,
            )
            .on_conflict_do_update(
                index_elements=(ContentSyncState.beatmapset_id,),
                set_={
                    "etag": snapshot.etag,
                    "last_modified": snapshot.last_modified,
                    "last_checked_at": now,
                    "next_check_at": now + timedelta(hours=24),
                    "unchanged_count": ContentSyncState.unchanged_count + unchanged_sync,
                    "error_count": 0,
                    "last_error": None,
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

    async def _ensure_media_asset(self, item: SyncedBeatmapFile) -> uuid.UUID:
        digest = item.stored_object.sha256
        assert digest is not None
        asset_id = await self._session.scalar(select(MediaAsset.id).where(MediaAsset.sha256 == digest))
        if asset_id is not None:
            return asset_id
        asset_id = await self._session.scalar(
            insert(MediaAsset)
            .values(
                id=item.asset_id,
                storage_key=item.stored_object.storage_key,
                sha256=digest,
                media_type=item.stored_object.media_type,
                size_bytes=item.stored_object.size_bytes,
            )
            .on_conflict_do_nothing()
            .returning(MediaAsset.id)
        )
        if asset_id is None:
            asset_id = await self._session.scalar(select(MediaAsset.id).where(MediaAsset.sha256 == digest))
        if asset_id is None:
            raise RuntimeError("media asset conflict did not resolve by digest")
        return asset_id


def _revision_statement() -> Select:
    no_mod_stars = (
        select(func.max(BeatmapDifficultyAttribute.star_rating))
        .join(ModSet, ModSet.id == BeatmapDifficultyAttribute.mod_set_id)
        .join(Scoreboard, Scoreboard.id == BeatmapDifficultyAttribute.scoreboard_id)
        .where(
            BeatmapDifficultyAttribute.beatmap_revision_id == BeatmapRevision.id,
            ModSet.legacy_bits == 0,
            Scoreboard.variant == "vanilla",
        )
        .correlate(BeatmapRevision)
        .scalar_subquery()
    )
    return (
        select(
            Beatmap.id.label("beatmap_id"),
            Beatmap.external_id.label("external_beatmap_id"),
            Beatmapset.id.label("beatmapset_id"),
            Beatmapset.external_id.label("external_beatmapset_id"),
            BeatmapRevision.id.label("revision_id"),
            BeatmapRevision.md5,
            BeatmapRevision.sha256,
            BeatmapRevision.file_name,
            Beatmapset.artist,
            Beatmapset.title,
            Beatmapset.creator_name,
            Beatmap.difficulty_name,
            Beatmap.ruleset,
            Beatmap.status,
            BeatmapRevision.source_updated_at,
            BeatmapRevision.total_length_ms,
            BeatmapRevision.drain_length_ms,
            BeatmapRevision.bpm,
            BeatmapRevision.circle_size,
            BeatmapRevision.overall_difficulty,
            BeatmapRevision.approach_rate,
            BeatmapRevision.health_drain,
            BeatmapRevision.object_count,
            BeatmapRevision.max_combo,
            no_mod_stars.label("star_rating"),
            BeatmapRevision.has_video,
            BeatmapRevision.is_current,
            MediaAsset.storage_key.label("file_storage_key"),
            MediaAsset.media_type.label("file_media_type"),
            MediaAsset.size_bytes.label("file_size_bytes"),
            Beatmapset.last_source_update_at,
            Beatmapset.available,
        )
        .select_from(BeatmapRevision)
        .join(Beatmap, Beatmap.id == BeatmapRevision.beatmap_id)
        .join(Beatmapset, Beatmapset.id == Beatmap.beatmapset_id)
        .outerjoin(MediaAsset, MediaAsset.id == BeatmapRevision.file_asset_id)
    )


def _revision_view(row: object | None) -> BeatmapRevisionView | None:
    return None if row is None else _required_revision_view(row)


def _required_revision_view(row: object) -> BeatmapRevisionView:
    return BeatmapRevisionView(
        beatmap_id=row.beatmap_id,  # type: ignore[attr-defined]
        external_beatmap_id=row.external_beatmap_id,  # type: ignore[attr-defined]
        beatmapset_id=row.beatmapset_id,  # type: ignore[attr-defined]
        external_beatmapset_id=row.external_beatmapset_id,  # type: ignore[attr-defined]
        revision_id=row.revision_id,  # type: ignore[attr-defined]
        md5=row.md5,  # type: ignore[attr-defined]
        sha256=row.sha256,  # type: ignore[attr-defined]
        file_name=row.file_name,  # type: ignore[attr-defined]
        artist=row.artist,  # type: ignore[attr-defined]
        title=row.title,  # type: ignore[attr-defined]
        creator=row.creator_name,  # type: ignore[attr-defined]
        difficulty_name=row.difficulty_name,  # type: ignore[attr-defined]
        ruleset=row.ruleset.value,  # type: ignore[attr-defined]
        status=row.status.value,  # type: ignore[attr-defined]
        source_updated_at=row.source_updated_at,  # type: ignore[attr-defined]
        total_length_ms=row.total_length_ms,  # type: ignore[attr-defined]
        drain_length_ms=row.drain_length_ms,  # type: ignore[attr-defined]
        bpm=row.bpm,  # type: ignore[attr-defined]
        circle_size=row.circle_size,  # type: ignore[attr-defined]
        overall_difficulty=row.overall_difficulty,  # type: ignore[attr-defined]
        approach_rate=row.approach_rate,  # type: ignore[attr-defined]
        health_drain=row.health_drain,  # type: ignore[attr-defined]
        object_count=row.object_count,  # type: ignore[attr-defined]
        max_combo=row.max_combo,  # type: ignore[attr-defined]
        star_rating=row.star_rating,  # type: ignore[attr-defined]
        has_video=row.has_video,  # type: ignore[attr-defined]
        is_current=row.is_current,  # type: ignore[attr-defined]
        file_storage_key=row.file_storage_key,  # type: ignore[attr-defined]
        file_media_type=row.file_media_type,  # type: ignore[attr-defined]
        file_size_bytes=row.file_size_bytes,  # type: ignore[attr-defined]
    )


def _beatmapset_view(rows: Sequence[object]) -> BeatmapsetView | None:
    return None if not rows else _required_beatmapset_view(rows)


def _required_beatmapset_view(rows: Sequence[object]) -> BeatmapsetView:
    first = rows[0]
    beatmaps = tuple(_required_revision_view(row) for row in rows)
    return BeatmapsetView(
        beatmapset_id=first.beatmapset_id,  # type: ignore[attr-defined]
        external_beatmapset_id=first.external_beatmapset_id,  # type: ignore[attr-defined]
        artist=first.artist,  # type: ignore[attr-defined]
        title=first.title,  # type: ignore[attr-defined]
        creator=first.creator_name,  # type: ignore[attr-defined]
        status=first.status.value,  # type: ignore[attr-defined]
        last_updated_at=first.last_source_update_at,  # type: ignore[attr-defined]
        available=first.available,  # type: ignore[attr-defined]
        has_video=any(item.has_video for item in beatmaps),
        beatmaps=beatmaps,
    )
