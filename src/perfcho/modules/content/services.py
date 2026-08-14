"""Provide canonical beatmap content queries and community writes."""

import asyncio
import hashlib
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Protocol

from perfcho.infra.cache import cached
from perfcho.infra.cache.backend import CacheBackend
from perfcho.infra.cache.values import decode_json, encode_json
from perfcho.infra.db.enums import BeatmapStatus, BeatmapStatusEventSource
from perfcho.infra.logging import duration_ms, log_event
from perfcho.modules.common.models import PendingEvent
from perfcho.modules.common.ports import Clock, IdGenerator, ObjectStorage, OutboxWriterFactory
from perfcho.modules.content.errors import (
    BeatmapNotFound,
    BeatmapsetNotFound,
    ContentInputRejected,
    InvalidStatusTransition,
    UpstreamContentUnavailable,
)
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
from perfcho.modules.content.ports import (
    ContentRepositoryFactory,
    ContentUnitOfWork,
    UpstreamContentSource,
)
from perfcho.modules.content.status import is_valid_transition

_REFRESH_LEASE = timedelta(minutes=30)
_REFRESH_RETRY = timedelta(minutes=5)
_LEADERBOARD_STATUSES = frozenset({"ranked", "approved", "loved"})


class _CacheOwner(Protocol):
    _cache: CacheBackend


def _beatmap_view(value: object) -> BeatmapRevisionView:
    if not isinstance(value, dict):
        raise ValueError("invalid cached beatmap")
    return BeatmapRevisionView(**value)


def _beatmapset_view(value: object) -> BeatmapsetView:
    if not isinstance(value, dict):
        raise ValueError("invalid cached beatmapset")
    return BeatmapsetView(
        beatmapset_id=value["beatmapset_id"],
        external_beatmapset_id=value["external_beatmapset_id"],
        artist=value["artist"],
        title=value["title"],
        creator=value["creator"],
        status=value["status"],
        last_updated_at=value["last_updated_at"],
        available=value["available"],
        has_video=value["has_video"],
        beatmaps=tuple(_beatmap_view(item) for item in value["beatmaps"]),
    )


def _content_md5_key(self: _CacheOwner, md5: str | bytes) -> str:
    return self._cache.key("content", "md5", _md5_bytes(md5).hex())


def _content_beatmap_key(self: _CacheOwner, beatmap_id: int, *, external: bool = True) -> str:
    return self._cache.key("content", "beatmap", f"{int(external)}:{beatmap_id}")


def _content_filename_key(self: _CacheOwner, file_name: str) -> str:
    normalized = _filename_key(file_name)
    return self._cache.key("content", "filename", hashlib.sha256(normalized.encode()).hexdigest())


def _content_beatmapset_key(self: _CacheOwner, beatmapset_id: int, *, external: bool = True) -> str:
    return self._cache.key("content", "beatmapset", f"{int(external)}:{beatmapset_id}")


class ContentQueryService:
    """Read canonical immutable content through short-lived sessions."""

    def __init__(
        self,
        uow_factory: Callable[[], ContentUnitOfWork],
        repository_factory: ContentRepositoryFactory,
        cache: CacheBackend,
    ) -> None:
        """Bind transaction and persistence factories."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._cache = cache

    @cached(
        key_builder=_content_md5_key,
        encode=encode_json,
        decode=lambda raw: _beatmap_view(decode_json(raw)),
        ttl_seconds=900,
    )
    async def lookup_md5(self, md5: str | bytes) -> BeatmapRevisionView:
        """Resolve an immutable revision by its MD5 checksum."""
        digest = _md5_bytes(md5)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).lookup_md5(digest)
        if result is None:
            raise BeatmapNotFound("beatmap md5 is unknown")
        return result

    @cached(
        key_builder=_content_beatmap_key,
        encode=encode_json,
        decode=lambda raw: _beatmap_view(decode_json(raw)),
        ttl_seconds=300,
    )
    async def lookup_beatmap(self, beatmap_id: int, *, external: bool = True) -> BeatmapRevisionView:
        """Resolve the current revision by public or canonical beatmap ID."""
        _positive("beatmap_id", beatmap_id)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).lookup_beatmap(beatmap_id, external=external)
        if result is None:
            raise BeatmapNotFound("beatmap is unknown")
        return result

    @cached(
        key_builder=_content_filename_key,
        encode=encode_json,
        decode=lambda raw: _beatmap_view(decode_json(raw)),
        ttl_seconds=300,
    )
    async def lookup_filename(self, file_name: str) -> BeatmapRevisionView:
        """Resolve the current revision by normalized filename."""
        normalized = _filename_key(file_name)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).lookup_filename(normalized)
        if result is None:
            raise BeatmapNotFound("beatmap filename is unknown")
        return result

    async def batch_lookup(
        self,
        file_names: tuple[str, ...],
        external_beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapRevisionView, ...]:
        """Resolve a mixed beatmap selector batch in one repository round trip."""
        if len(file_names) + len(external_beatmap_ids) > 2048:
            raise ContentInputRejected("beatmap batch exceeds 2048 selectors")
        keys = tuple(_filename_key(name) for name in file_names)
        if any(identifier < 1 for identifier in external_beatmap_ids):
            raise ContentInputRejected("beatmap IDs must be positive")
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).batch_lookup(keys, external_beatmap_ids)

    @cached(
        key_builder=_content_beatmapset_key,
        encode=encode_json,
        decode=lambda raw: _beatmapset_view(decode_json(raw)),
        ttl_seconds=300,
    )
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

    async def list_comments(self, target: str, external_target_id: int) -> tuple[CommentView, ...]:
        """List visible position-aware comments for one content target."""
        _comment_target(target)
        _positive("external_target_id", external_target_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_comments(target, external_target_id)


class ContentService:
    """Mutate favourite, rating, and ranking-status facts in explicit short transactions."""

    def __init__(
        self,
        uow_factory: Callable[[], ContentUnitOfWork],
        repository_factory: ContentRepositoryFactory,
        outbox_writer_factory: OutboxWriterFactory,
        clock: Clock,
    ) -> None:
        """Bind transaction, persistence, event, and time dependencies."""
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._clock = clock

    async def set_favourite(self, account_id: int, beatmapset_id: int, favourited: bool = True) -> FavouriteResult:
        """Set a naturally idempotent account favourite."""
        started_ns = time.monotonic_ns()
        _positive("account_id", account_id)
        _positive("beatmapset_id", beatmapset_id)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).set_favourite(account_id, beatmapset_id, favourited)
            await uow.commit()
            log_event(
                "DEBUG",
                "content.favourite.changed",
                account_id=result.account_id,
                beatmapset_id=result.beatmapset_id,
                favourited=result.favourited,
                changed=result.changed,
                duration_ms=duration_ms(started_ns),
            )
            return result

    async def rate(self, account_id: int, beatmap_id: int, rating: int) -> RatingSummary:
        """Upsert one bounded logical-beatmap rating and return the new aggregate."""
        started_ns = time.monotonic_ns()
        _positive("account_id", account_id)
        _positive("beatmap_id", beatmap_id)
        if isinstance(rating, bool) or not 1 <= rating <= 10:
            raise ContentInputRejected("rating must be between 1 and 10")
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).rate(account_id, beatmap_id, rating)
            await uow.commit()
            log_event(
                "DEBUG",
                "content.rating.changed",
                account_id=account_id,
                beatmap_id=result.beatmap_id,
                rating=result.account_rating,
                duration_ms=duration_ms(started_ns),
            )
            return result

    async def create_comment(
        self,
        account_id: int,
        target: str,
        external_target_id: int,
        position_ms: int,
        body: str,
    ) -> CommentView:
        """Persist one bounded position-aware content comment."""
        _positive("account_id", account_id)
        _positive("external_target_id", external_target_id)
        _comment_target(target)
        content = body.strip()
        if not content or len(content) > 1000 or position_ms < 0:
            raise ContentInputRejected("comment is invalid")
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).create_comment(
                account_id,
                target,
                external_target_id,
                position_ms,
                content,
            )
            await uow.commit()
            return result

    async def qualify(self, actor_account_id: int, beatmapset_id: int) -> BeatmapsetStatusState:
        """Transition a pending beatmapset to qualified."""
        return await self._transition(
            actor_account_id, beatmapset_id, BeatmapStatus.QUALIFIED, BeatmapStatusEventSource.QUALIFICATION
        )

    async def disqualify(
        self, actor_account_id: int, beatmapset_id: int, reason: str | None = None
    ) -> BeatmapsetStatusState:
        """Transition a qualified beatmapset back to pending."""
        return await self._transition(
            actor_account_id, beatmapset_id, BeatmapStatus.PENDING, BeatmapStatusEventSource.DISQUALIFICATION, reason
        )

    async def rank(self, actor_account_id: int, beatmapset_id: int) -> BeatmapsetStatusState:
        """Transition a qualified beatmapset to ranked."""
        return await self._transition(
            actor_account_id, beatmapset_id, BeatmapStatus.RANKED, BeatmapStatusEventSource.RANK
        )

    async def love(self, actor_account_id: int, beatmapset_id: int) -> BeatmapsetStatusState:
        """Transition a ranked beatmapset to loved."""
        return await self._transition(
            actor_account_id, beatmapset_id, BeatmapStatus.LOVED, BeatmapStatusEventSource.LOVE
        )

    async def unlove(self, actor_account_id: int, beatmapset_id: int) -> BeatmapsetStatusState:
        """Transition a loved beatmapset back to ranked."""
        return await self._transition(
            actor_account_id, beatmapset_id, BeatmapStatus.RANKED, BeatmapStatusEventSource.UNLOVE
        )

    async def unrank(self, actor_account_id: int, beatmapset_id: int) -> BeatmapsetStatusState:
        """Transition a ranked beatmapset to graveyard."""
        return await self._transition(
            actor_account_id, beatmapset_id, BeatmapStatus.GRAVEYARD, BeatmapStatusEventSource.GRAVEYARD
        )

    async def restore_pending(self, actor_account_id: int, beatmapset_id: int) -> BeatmapsetStatusState:
        """Transition a graveyard beatmapset back to pending."""
        return await self._transition(
            actor_account_id, beatmapset_id, BeatmapStatus.PENDING, BeatmapStatusEventSource.GRAVEYARD
        )

    async def abandon(self, actor_account_id: int, beatmapset_id: int) -> BeatmapsetStatusState:
        """Transition a wip or pending beatmapset to graveyard."""
        return await self._transition(
            actor_account_id, beatmapset_id, BeatmapStatus.GRAVEYARD, BeatmapStatusEventSource.GRAVEYARD
        )

    async def withdraw(self, actor_account_id: int, beatmapset_id: int) -> BeatmapsetStatusState:
        """Transition a pending beatmapset back to wip."""
        return await self._transition(
            actor_account_id, beatmapset_id, BeatmapStatus.WIP, BeatmapStatusEventSource.GRAVEYARD
        )

    async def revert_status(self, actor_account_id: int, beatmapset_id: int) -> BeatmapsetStatusState:
        """Restore the authoritative status to the upstream source status."""
        _positive("beatmapset_id", beatmapset_id)
        del actor_account_id
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            state = await repository.get_status_state(beatmapset_id, for_update=True)
            if state is None:
                raise BeatmapsetNotFound("beatmapset is unknown")
            if state.status == state.source_status:
                await uow.commit()
                return state
            effective_at = self._clock.now()
            await repository.revert_status(
                state.beatmapset_id, source=BeatmapStatusEventSource.REVERT, effective_at=effective_at
            )
            await self._outbox_writer_factory(uow.session).append(
                _status_changed_event(
                    state.beatmapset_id,
                    state.external_beatmapset_id,
                    state.status,
                    state.source_status,
                    BeatmapStatusEventSource.REVERT,
                    None,
                    effective_at,
                )
            )
            await uow.commit()
        return BeatmapsetStatusState(
            beatmapset_id=state.beatmapset_id,
            external_beatmapset_id=state.external_beatmapset_id,
            status=state.source_status,
            source_status=state.source_status,
        )

    async def list_status_events(self, beatmapset_id: int) -> tuple[BeatmapsetStatusEventView, ...]:
        """List a beatmapset's status transitions."""
        _positive("beatmapset_id", beatmapset_id)
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).list_status_events(beatmapset_id)

    async def _transition(
        self,
        actor_account_id: int,
        beatmapset_id: int,
        target_status: BeatmapStatus,
        source: BeatmapStatusEventSource,
        reason: str | None = None,
    ) -> BeatmapsetStatusState:
        """Validate and atomically persist one ranking status transition."""
        _positive("beatmapset_id", beatmapset_id)
        async with self._uow_factory() as uow:
            repository = self._repository_factory(uow.session)
            state = await repository.get_status_state(beatmapset_id, for_update=True)
            if state is None:
                raise BeatmapsetNotFound("beatmapset is unknown")
            current_status = BeatmapStatus(state.status)
            if current_status == target_status:
                await uow.commit()
                return state
            if not is_valid_transition(state.status, target_status.value):
                raise InvalidStatusTransition(
                    f"cannot transition beatmapset from {state.status} to {target_status.value}"
                )
            effective_at = self._clock.now()
            await repository.apply_status_transition(
                state.beatmapset_id,
                previous_status=current_status,
                target_status=target_status,
                source=source,
                actor_account_id=actor_account_id,
                reason=reason,
                effective_at=effective_at,
            )
            await self._outbox_writer_factory(uow.session).append(
                _status_changed_event(
                    state.beatmapset_id,
                    state.external_beatmapset_id,
                    state.status,
                    target_status.value,
                    source,
                    actor_account_id,
                    effective_at,
                )
            )
            await uow.commit()
        return BeatmapsetStatusState(
            beatmapset_id=state.beatmapset_id,
            external_beatmapset_id=state.external_beatmapset_id,
            status=target_status.value,
            source_status=state.source_status,
        )


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
        cache: CacheBackend,
        *,
        max_concurrency: int = 8,
    ) -> None:
        """Bind upstream, storage, transaction, event, time, and ID dependencies."""
        _positive("max_concurrency", max_concurrency)
        self._uow_factory = uow_factory
        self._repository_factory = repository_factory
        self._outbox_writer_factory = outbox_writer_factory
        self._upstream = upstream
        self._object_storage = object_storage
        self._clock = clock
        self._id_generator = id_generator
        self._cache = cache
        self._io_semaphore = asyncio.Semaphore(max_concurrency)
        self._inflight_lock = asyncio.Lock()
        self._inflight: dict[int, asyncio.Task[ContentSyncResult]] = {}
        self._closing = False

    async def aclose(self) -> None:
        """Cancel and drain process-owned synchronization tasks before infrastructure closes."""
        async with self._inflight_lock:
            self._closing = True
            tasks = tuple(self._inflight.values())
        for task in tasks:
            task.cancel()
        if tasks:
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)
            for outcome in outcomes:
                if isinstance(outcome, BaseException) and not isinstance(outcome, asyncio.CancelledError):
                    log_event("ERROR", "content.sync.shutdown_failed", exception=outcome)

    async def resolve_revision(
        self,
        md5: str | bytes,
        file_name: str,
        external_beatmapset_id: int | None,
    ) -> BeatmapRevisionView:
        """Resolve a local revision, synchronizing authoritative content only on a cache miss."""
        digest = _md5_bytes(md5)
        file_name_key = _filename_key(file_name)
        local = await self._lookup_md5(digest)
        if local is not None:
            return local

        set_id = external_beatmapset_id
        if set_id is None:
            set_id = await self._upstream.lookup_beatmapset_id(digest.hex(), file_name)
        _positive("external_beatmapset_id", set_id)
        await self._synchronize_once(set_id)

        exact = await self._lookup_md5(digest)
        if exact is not None:
            return exact
        beatmapset = await self._get_beatmapset(set_id)
        if beatmapset is not None:
            for beatmap in beatmapset.beatmaps:
                if _filename_key(beatmap.file_name) == file_name_key:
                    return beatmap
        raise BeatmapNotFound("beatmap is unknown after upstream synchronization")

    async def refresh_if_due(self, external_beatmapset_id: int) -> None:
        """Refresh one known set after its response path, suppressing expected background failures."""
        _positive("external_beatmapset_id", external_beatmapset_id)
        now = self._clock.now()
        lease_until = now + _REFRESH_LEASE
        try:
            async with self._uow_factory() as uow:
                claimed = await self._repository_factory(uow.session).claim_beatmapset_refresh(
                    external_beatmapset_id,
                    now=now,
                    lease_until=lease_until,
                )
                await uow.commit()
        except Exception as error:
            log_event(
                "WARNING",
                "content.sync.background_claim_failed",
                exception=error,
                external_beatmapset_id=external_beatmapset_id,
                error_type=type(error).__name__,
            )
            return
        if not claimed:
            return
        try:
            await self._synchronize_once(external_beatmapset_id)
        except Exception as error:
            failed_at = self._clock.now()
            try:
                async with self._uow_factory() as uow:
                    await self._repository_factory(uow.session).record_beatmapset_refresh_failure(
                        external_beatmapset_id,
                        expected_lease_until=lease_until,
                        checked_at=failed_at,
                        next_check_at=failed_at + _REFRESH_RETRY,
                        error=getattr(error, "code", type(error).__name__),
                    )
                    await uow.commit()
            except Exception as persistence_error:
                log_event(
                    "WARNING",
                    "content.sync.background_failure_record_failed",
                    exception=persistence_error,
                    external_beatmapset_id=external_beatmapset_id,
                    error_type=type(persistence_error).__name__,
                )
            log_event(
                "WARNING",
                "content.sync.background_failed",
                exception=error,
                external_beatmapset_id=external_beatmapset_id,
                error_type=type(error).__name__,
            )

    async def synchronize(self, external_beatmapset_id: int) -> ContentSyncResult:
        """Synchronize one upstream set without holding a database transaction during I/O."""
        started_ns = time.monotonic_ns()
        _positive("external_beatmapset_id", external_beatmapset_id)
        log_event(
            "INFO",
            "content.sync.started",
            external_beatmapset_id=external_beatmapset_id,
        )
        snapshot = await self._upstream.fetch_beatmapset(external_beatmapset_id)
        if snapshot.external_beatmapset_id != external_beatmapset_id:
            raise ContentInputRejected("upstream beatmapset identity does not match the request")

        async def synchronize_file(beatmap: UpstreamBeatmapSnapshot) -> SyncedBeatmapFile:
            async with self._io_semaphore:
                return await self._fetch_and_store_file(snapshot, beatmap)

        files = await asyncio.gather(*(synchronize_file(beatmap) for beatmap in snapshot.beatmaps))

        now = self._clock.now()
        next_check_at = _next_check_at(snapshot, now)
        async with self._uow_factory() as uow:
            result = await self._repository_factory(uow.session).synchronize_beatmapset(
                snapshot,
                tuple(files),
                now=now,
                next_check_at=next_check_at,
            )
            if not result.published:
                await uow.commit()
                log_event(
                    "DEBUG",
                    "content.sync.stale_ignored",
                    beatmapset_id=result.beatmapset_id,
                    external_beatmapset_id=result.external_beatmapset_id,
                    duration_ms=duration_ms(started_ns),
                )
                return result
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
                    consumers=("content-consumer.v1",),
                    partition_key=f"beatmapset:{result.beatmapset_id}",
                )
            )
            await uow.commit()
            await self._invalidate_snapshot(snapshot)
            log_event(
                "INFO",
                "content.sync.committed",
                beatmapset_id=result.beatmapset_id,
                external_beatmapset_id=result.external_beatmapset_id,
                created_revision_count=result.created_revision_count,
                unchanged_revision_count=result.unchanged_revision_count,
                removed_beatmap_count=result.removed_beatmap_count,
                duration_ms=duration_ms(started_ns),
            )
            return result

    async def _invalidate_snapshot(self, snapshot: UpstreamBeatmapsetSnapshot) -> None:
        """Remove cache entries affected by a committed upstream snapshot."""
        await self._cache.delete(self._cache.key("content", "beatmapset", f"1:{snapshot.external_beatmapset_id}"))
        for beatmap in snapshot.beatmaps:
            await self._cache.delete(self._cache.key("content", "beatmap", f"1:{beatmap.external_beatmap_id}"))
            await self._cache.delete(self._cache.key("content", "md5", beatmap.md5.hex()))
            filename = hashlib.sha256(_filename_key(beatmap.file_name).encode()).hexdigest()
            await self._cache.delete(self._cache.key("content", "filename", filename))

    async def _fetch_and_store_file(
        self,
        snapshot: UpstreamBeatmapsetSnapshot,
        beatmap: UpstreamBeatmapSnapshot,
    ) -> SyncedBeatmapFile:
        """Fetch, verify, and store one beatmap while holding a bounded I/O slot."""
        content = await self._upstream.fetch_beatmap_file(beatmap.external_beatmap_id)
        if len(content) >= 256 * 1024:
            md5, sha256 = await asyncio.to_thread(_content_digests, content)
        else:
            md5, sha256 = _content_digests(content)
        if md5 != beatmap.md5:
            raise UpstreamContentUnavailable("upstream beatmap file does not match its MD5 metadata")
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
        return SyncedBeatmapFile(beatmap, self._id_generator.new(), stored)

    async def _synchronize_once(self, external_beatmapset_id: int) -> ContentSyncResult:
        async with self._inflight_lock:
            if self._closing:
                raise UpstreamContentUnavailable("content synchronization is shutting down")
            task = self._inflight.get(external_beatmapset_id)
            if task is None:
                task = asyncio.create_task(
                    self._run_synchronize(external_beatmapset_id),
                    name=f"content-sync-{external_beatmapset_id}",
                )
                task.add_done_callback(_consume_task_result)
                self._inflight[external_beatmapset_id] = task
        return await asyncio.shield(task)

    async def _run_synchronize(self, external_beatmapset_id: int) -> ContentSyncResult:
        try:
            return await self.synchronize(external_beatmapset_id)
        finally:
            current = asyncio.current_task()
            async with self._inflight_lock:
                if self._inflight.get(external_beatmapset_id) is current:
                    del self._inflight[external_beatmapset_id]

    async def _lookup_md5(self, digest: bytes) -> BeatmapRevisionView | None:
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).lookup_md5(digest)

    async def _get_beatmapset(self, external_beatmapset_id: int) -> BeatmapsetView | None:
        async with self._uow_factory() as uow:
            return await self._repository_factory(uow.session).get_beatmapset(
                external_beatmapset_id,
                external=True,
            )


def _content_digests(content: bytes) -> tuple[bytes, bytes]:
    return hashlib.md5(content, usedforsecurity=False).digest(), hashlib.sha256(content).digest()


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


def _next_check_at(snapshot: UpstreamBeatmapsetSnapshot, now: datetime) -> datetime:
    newest_update = max(beatmap.source_updated_at for beatmap in snapshot.beatmaps)
    update_age = max(0, (now - newest_update).days)
    check_hours = 2 + (5 / 365) * update_age
    if any(beatmap.status in _LEADERBOARD_STATUSES for beatmap in snapshot.beatmaps):
        check_hours *= 4
    return now + min(timedelta(hours=check_hours), timedelta(days=1))


def _consume_task_result(task: asyncio.Task[ContentSyncResult]) -> None:
    if not task.cancelled():
        task.exception()


def _positive(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ContentInputRejected(f"{name} must be a positive integer")


def _comment_target(value: str) -> None:
    if value not in {"map", "song", "replay"}:
        raise ContentInputRejected("comment target is invalid")


def _status_changed_event(
    beatmapset_id: int,
    external_beatmapset_id: int,
    previous_status: str,
    status: str,
    source: BeatmapStatusEventSource,
    actor_account_id: int | None,
    effective_at: datetime,
) -> PendingEvent:
    return PendingEvent(
        aggregate_type="beatmapset",
        aggregate_id=str(beatmapset_id),
        event_type="content.beatmapset-status-changed.v1",
        schema_version=1,
        payload={
            "beatmapset_id": beatmapset_id,
            "external_beatmapset_id": external_beatmapset_id,
            "previous_status": previous_status,
            "status": status,
            "source": source.value,
            "actor_account_id": actor_account_id,
            "effective_at": effective_at.isoformat(),
        },
        consumers=("content-consumer.v1",),
        partition_key=f"beatmapset:{beatmapset_id}",
    )
