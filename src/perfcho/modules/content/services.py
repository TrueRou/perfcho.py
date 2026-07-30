"""Provide canonical beatmap content queries and community writes."""

import hashlib
from collections.abc import Callable

from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import Clock, IdGenerator, ObjectStorage, OutboxWriterFactory
from perfcho.modules.content.errors import BeatmapNotFound, BeatmapsetNotFound, ContentInputRejected
from perfcho.modules.content.models import (
    BeatmapRevisionView,
    BeatmapsetView,
    ContentSearch,
    ContentSearchPage,
    ContentSyncResult,
    FavouriteResult,
    RatingSummary,
    SyncedBeatmapFile,
)
from perfcho.modules.content.ports import (
    ContentRepositoryFactory,
    ContentUnitOfWork,
    UpstreamContentSource,
)


class ContentQueryService:
    """Read canonical immutable content through short-lived sessions."""

    def __init__(
        self,
        uow_factory: Callable[[], ContentUnitOfWork],
        repository_factory: ContentRepositoryFactory,
    ) -> None:
        """Bind transaction and persistence factories."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory

    async def lookup_md5(self, md5: str | bytes) -> BeatmapRevisionView:
        """Resolve an immutable revision by Stable MD5."""
        digest = _md5_bytes(md5)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).lookup_md5(digest)
        if result is None:
            raise BeatmapNotFound("beatmap md5 is unknown")
        return result

    async def lookup_beatmap(self, beatmap_id: int, *, external: bool = True) -> BeatmapRevisionView:
        """Resolve the current revision by public or canonical beatmap ID."""
        _positive("beatmap_id", beatmap_id)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).lookup_beatmap(beatmap_id, external=external)
        if result is None:
            raise BeatmapNotFound("beatmap is unknown")
        return result

    async def lookup_filename(self, file_name: str) -> BeatmapRevisionView:
        """Resolve the current revision by normalized Stable filename."""
        key = _filename_key(file_name)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).lookup_filename(key)
        if result is None:
            raise BeatmapNotFound("beatmap filename is unknown")
        return result

    async def batch_lookup(
        self,
        file_names: tuple[str, ...],
        external_beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapRevisionView, ...]:
        """Resolve Stable song-select maps in one repository round trip."""
        if len(file_names) + len(external_beatmap_ids) > 2048:
            raise ContentInputRejected("beatmap batch exceeds 2048 selectors")
        keys = tuple(_filename_key(name) for name in file_names)
        if any(identifier < 1 for identifier in external_beatmap_ids):
            raise ContentInputRejected("beatmap IDs must be positive")
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).batch_lookup(keys, external_beatmap_ids)

    async def get_beatmapset(self, beatmapset_id: int, *, external: bool = True) -> BeatmapsetView:
        """Return a beatmapset with all current revisions."""
        _positive("beatmapset_id", beatmapset_id)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).get_beatmapset(beatmapset_id, external=external)
        if result is None:
            raise BeatmapsetNotFound("beatmapset is unknown")
        return result

    async def search(self, query: ContentSearch) -> ContentSearchPage:
        """Search locally indexed beatmapsets without upstream side effects."""
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).search(query)

    async def list_favourites(self, account_id: int) -> tuple[int, ...]:
        """Return public beatmapset IDs favourited by an account."""
        _positive("account_id", account_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_favourites(account_id)

    async def get_rating(self, beatmap_id: int, account_id: int | None = None) -> RatingSummary:
        """Return aggregate and optional account rating state for a logical beatmap."""
        _positive("beatmap_id", beatmap_id)
        if account_id is not None:
            _positive("account_id", account_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).get_rating(beatmap_id, account_id)


class ContentService:
    """Mutate favourite and rating facts in explicit short transactions."""

    def __init__(
        self,
        uow_factory: Callable[[], ContentUnitOfWork],
        repository_factory: ContentRepositoryFactory,
    ) -> None:
        """Bind transaction and persistence factories."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory

    async def set_favourite(self, account_id: int, beatmapset_id: int, favourited: bool = True) -> FavouriteResult:
        """Set a naturally idempotent account favourite."""
        _positive("account_id", account_id)
        _positive("beatmapset_id", beatmapset_id)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).set_favourite(account_id, beatmapset_id, favourited)
            await uow.commit()
            return result

    async def rate(self, account_id: int, beatmap_id: int, rating: int) -> RatingSummary:
        """Upsert one bounded logical-beatmap rating and return the new aggregate."""
        _positive("account_id", account_id)
        _positive("beatmap_id", beatmap_id)
        if isinstance(rating, bool) or not 1 <= rating <= 10:
            raise ContentInputRejected("rating must be between 1 and 10")
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).rate(account_id, beatmap_id, rating)
            await uow.commit()
            return result


class ContentSyncService:
    """Fetch, verify, store, and atomically publish immutable beatmap revisions."""

    def __init__(
        self,
        uow_factory: Callable[[], ContentUnitOfWork],
        repository_factory: ContentRepositoryFactory,
        outbox_writer_factory: OutboxWriterFactory,
        upstream: UpstreamContentSource,
        object_storage: ObjectStorage,
        clock: Clock,
        id_generator: IdGenerator,
    ) -> None:
        """Bind upstream, storage, transaction, event, time, and ID dependencies."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._upstream = upstream
        self._object_storage = object_storage
        self._clock = clock
        self._id_generator = id_generator

    async def synchronize(self, external_beatmapset_id: int) -> ContentSyncResult:
        """Synchronize one upstream set without holding a database transaction during I/O."""
        _positive("external_beatmapset_id", external_beatmapset_id)
        snapshot = await self._upstream.fetch_beatmapset(external_beatmapset_id)
        if snapshot.external_beatmapset_id != external_beatmapset_id:
            raise ContentInputRejected("upstream beatmapset identity does not match the request")

        files: list[SyncedBeatmapFile] = []
        for beatmap in snapshot.beatmaps:
            content = await self._upstream.fetch_beatmap_file(beatmap.external_beatmap_id)
            if hashlib.md5(content, usedforsecurity=False).digest() != beatmap.md5:
                raise ContentInputRejected("upstream beatmap file does not match its MD5 metadata")
            sha256 = hashlib.sha256(content).digest()
            storage_key = (
                f"beatmaps/{snapshot.source_code}/{snapshot.external_beatmapset_id}/"
                f"{beatmap.external_beatmap_id}/{sha256.hex()}.osu"
            )
            stored = await self._object_storage.put(
                storage_key,
                content,
                media_type="application/x-osu-beatmap",
                expected_sha256=sha256,
            )
            files.append(SyncedBeatmapFile(beatmap, self._id_generator.new(), stored))

        now = self._clock.now()
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).synchronize_beatmapset(
                snapshot,
                tuple(files),
                now=now,
            )
            await self._outbox_writer_factory(uow.session).append(
                PendingEvent(
                    aggregate_type="beatmapset",
                    aggregate_id=str(result.beatmapset_id),
                    event_type="content.beatmapset-synchronized.v1",
                    schema_version=1,
                    payload={
                        "beatmapset_id": result.beatmapset_id,
                        "external_beatmapset_id": result.external_beatmapset_id,
                        "created_revision_count": result.created_revision_count,
                        "unchanged_revision_count": result.unchanged_revision_count,
                        "removed_beatmap_count": result.removed_beatmap_count,
                        "source_updated_at": snapshot.last_updated_at.isoformat(),
                    },
                    consumers=("content-projector.v1",),
                    partition_key=f"beatmapset:{result.beatmapset_id}",
                )
            )
            await uow.commit()
            return result


def _md5_bytes(value: str | bytes) -> bytes:
    if isinstance(value, bytes):
        if len(value) != 16:
            raise ContentInputRejected("beatmap md5 must contain 16 bytes")
        return value
    try:
        digest = bytes.fromhex(value)
    except ValueError as error:
        raise ContentInputRejected("beatmap md5 must be hexadecimal") from error
    if len(digest) != 16 or len(value) != 32:
        raise ContentInputRejected("beatmap md5 must contain 32 hexadecimal characters")
    return digest


def _filename_key(value: str) -> str:
    key = value.strip().casefold()
    if not key or len(key) > 255 or "/" in key or "\\" in key:
        raise ContentInputRejected("beatmap filename is invalid")
    return key


def _positive(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ContentInputRejected(f"{name} must be a positive integer")
