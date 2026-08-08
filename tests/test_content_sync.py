import asyncio
import hashlib
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import TracebackType
from typing import cast

import httpx
import pytest
from sqlalchemy import event, func, select

from perfcho.infra.db import engine as infra_db
from perfcho.infra.db.models.content import Beatmap, BeatmapRevision, BeatmapStatusEvent, RatingVote
from perfcho.infra.db.repositories.content import (
    SqlAlchemyContentRepository,
    _snapshot_beatmap_metadata,
    _snapshot_beatmapset_metadata,
    _snapshot_extends_current_revision_set,
)
from perfcho.infra.upstream.bancho import BanchoUpstreamContentSource
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage, PendingEvent, StoredObject
from perfcho.modules.content import (
    BeatmapRevisionView,
    BeatmapsetView,
    ContentSyncResult,
    ContentSyncService,
    SyncedBeatmapFile,
    UpstreamBeatmapsetSnapshot,
    UpstreamBeatmapSnapshot,
    UpstreamContentUnavailable,
)
from perfcho.modules.content.ports import ContentRepository

NOW = datetime(2026, 7, 29, 12, 30, tzinfo=UTC)
BEATMAP_FILE = b"osu file format v14\n[General]\n"


class FixedClock:
    def now(self) -> datetime:
        return NOW


class FakeIds:
    def new(self) -> uuid.UUID:
        return uuid.uuid7()


class FakeUnitOfWork:
    def __init__(self, calls: list[str]) -> None:
        self.session = object()
        self.calls = calls
        self.committed = False

    async def __aenter__(self) -> FakeUnitOfWork:
        self.calls.append("transaction-enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.calls.append("transaction-exit")

    async def commit(self) -> None:
        self.committed = True
        self.calls.append("commit")


class FakeSource:
    def __init__(self, snapshot: UpstreamBeatmapsetSnapshot, content: bytes) -> None:
        self.snapshot = snapshot
        self.content = content
        self.calls: list[int] = []

    async def fetch_beatmapset(self, external_beatmapset_id: int) -> UpstreamBeatmapsetSnapshot:
        assert external_beatmapset_id == self.snapshot.external_beatmapset_id
        return self.snapshot

    async def lookup_beatmapset_id(self, checksum: str, file_name: str) -> int:
        del checksum, file_name
        return self.snapshot.external_beatmapset_id

    async def fetch_beatmap_file(self, external_beatmap_id: int) -> bytes:
        self.calls.append(external_beatmap_id)
        return self.content


class FakeStorage:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.keys: list[str] = []

    async def put(
        self,
        storage_key: str,
        content: bytes,
        *,
        media_type: str,
        expected_sha256: bytes | None = None,
    ) -> StoredObject:
        self.calls.append("object-put")
        self.keys.append(storage_key)
        assert media_type == "application/x-osu-beatmap"
        assert expected_sha256 == hashlib.sha256(content).digest()
        return StoredObject(storage_key, len(content), media_type, expected_sha256)


class FakeRepository:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.snapshot: UpstreamBeatmapsetSnapshot | None = None
        self.files: tuple[SyncedBeatmapFile, ...] = ()
        self.revisions: dict[bytes, BeatmapRevisionView] = {}
        self.beatmapset: BeatmapsetView | None = None
        self.refresh_claimed = False
        self.refresh_claims: list[int] = []
        self.refresh_failures: list[tuple[datetime, datetime, datetime, str]] = []
        self.published = True

    async def lookup_md5(self, md5: bytes) -> BeatmapRevisionView | None:
        return self.revisions.get(md5)

    async def get_beatmapset(self, beatmapset_id: int, *, external: bool) -> BeatmapsetView | None:
        assert external and beatmapset_id == 200
        return self.beatmapset

    async def claim_beatmapset_refresh(
        self,
        external_beatmapset_id: int,
        *,
        now: datetime,
        lease_until: datetime,
    ) -> bool:
        assert now == NOW and lease_until == NOW + timedelta(minutes=30)
        self.refresh_claims.append(external_beatmapset_id)
        return self.refresh_claimed

    async def record_beatmapset_refresh_failure(
        self,
        external_beatmapset_id: int,
        *,
        expected_lease_until: datetime,
        checked_at: datetime,
        next_check_at: datetime,
        error: str,
    ) -> None:
        assert external_beatmapset_id == 200
        self.refresh_failures.append((expected_lease_until, checked_at, next_check_at, error))

    async def synchronize_beatmapset(
        self,
        snapshot: UpstreamBeatmapsetSnapshot,
        files: tuple[SyncedBeatmapFile, ...],
        *,
        now: datetime,
        next_check_at: datetime,
    ) -> ContentSyncResult:
        assert now == NOW
        assert next_check_at == NOW + timedelta(hours=8)
        self.calls.append("repository")
        self.snapshot = snapshot
        self.files = files
        revisions = tuple(revision_view(item) for item in files)
        self.revisions = {revision.md5: revision for revision in revisions}
        self.beatmapset = BeatmapsetView(
            beatmapset_id=20,
            external_beatmapset_id=snapshot.external_beatmapset_id,
            artist=snapshot.artist,
            title=snapshot.title,
            creator=snapshot.creator_name,
            status=snapshot.status,
            last_updated_at=snapshot.last_updated_at,
            available=snapshot.available,
            has_video=any(revision.has_video for revision in revisions),
            beatmaps=revisions,
        )
        return ContentSyncResult(20, snapshot.external_beatmapset_id, len(files), 0, 0, self.published)


class FakeOutbox:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.events: list[PendingEvent] = []

    async def append(self, event: PendingEvent) -> uuid.UUID:
        self.calls.append("outbox")
        self.events.append(event)
        return uuid.uuid7()


def snapshot(content: bytes = BEATMAP_FILE) -> UpstreamBeatmapsetSnapshot:
    beatmap = UpstreamBeatmapSnapshot(
        external_beatmap_id=100,
        md5=hashlib.md5(content, usedforsecurity=False).digest(),
        file_name="Artist - Title (Creator) [Insane].osu",
        difficulty_name="Insane",
        ruleset="osu",
        status="ranked",
        source_updated_at=NOW,
        total_length_ms=120_000,
        drain_length_ms=100_000,
        bpm=Decimal(180),
        circle_size=Decimal(4),
        overall_difficulty=Decimal(8),
        approach_rate=Decimal(9),
        health_drain=Decimal(6),
        circle_count=300,
        slider_count=190,
        spinner_count=10,
        max_combo=750,
        star_rating=Decimal("5.25"),
        has_storyboard=False,
        has_video=True,
    )
    return UpstreamBeatmapsetSnapshot(
        source_code="osu",
        external_beatmapset_id=200,
        creator_external_id=12,
        creator_name="Creator",
        artist="Artist",
        artist_unicode=None,
        title="Title",
        title_unicode=None,
        source_text=None,
        tags="tag",
        genre_id=2,
        language_id=3,
        description="Description",
        status="ranked",
        submitted_at=NOW,
        ranked_at=NOW,
        last_updated_at=NOW,
        available=True,
        nsfw=False,
        beatmaps=(beatmap,),
        etag="set-etag",
    )


def revision_view(item: SyncedBeatmapFile) -> BeatmapRevisionView:
    beatmap = item.beatmap
    return BeatmapRevisionView(
        beatmap_id=10,
        external_beatmap_id=beatmap.external_beatmap_id,
        beatmapset_id=20,
        external_beatmapset_id=200,
        revision_id=30,
        md5=beatmap.md5,
        sha256=item.stored_object.sha256 or b"",
        file_name=beatmap.file_name,
        artist="Artist",
        title="Title",
        creator="Creator",
        difficulty_name=beatmap.difficulty_name,
        ruleset=beatmap.ruleset,
        status=beatmap.status,
        source_updated_at=beatmap.source_updated_at,
        total_length_ms=beatmap.total_length_ms,
        drain_length_ms=beatmap.drain_length_ms,
        bpm=beatmap.bpm,
        circle_size=beatmap.circle_size,
        overall_difficulty=beatmap.overall_difficulty,
        approach_rate=beatmap.approach_rate,
        health_drain=beatmap.health_drain,
        object_count=beatmap.object_count,
        max_combo=beatmap.max_combo,
        star_rating=beatmap.star_rating,
        has_video=beatmap.has_video,
        is_current=True,
        file_storage_key=item.stored_object.storage_key,
        file_media_type=item.stored_object.media_type,
        file_size_bytes=item.stored_object.size_bytes,
    )


def snapshot_files(
    contents: tuple[bytes, ...],
    *,
    beatmapset_id: int,
    updated_at: datetime = NOW,
) -> tuple[UpstreamBeatmapsetSnapshot, tuple[SyncedBeatmapFile, ...]]:
    template = snapshot(contents[0])
    beatmaps = tuple(
        replace(
            template.beatmaps[0],
            external_beatmap_id=beatmapset_id * 1000 + index,
            md5=hashlib.md5(content, usedforsecurity=False).digest(),
            file_name=f"Artist - Title (Creator) [Difficulty {index}].osu",
            difficulty_name=f"Difficulty {index}",
            source_updated_at=updated_at,
        )
        for index, content in enumerate(contents)
    )
    beatmapset = replace(
        template,
        external_beatmapset_id=beatmapset_id,
        last_updated_at=updated_at,
        beatmaps=beatmaps,
    )
    files = tuple(
        SyncedBeatmapFile(
            beatmap,
            uuid.uuid7(),
            StoredObject(
                f"beatmaps/osu/{beatmapset_id}/{beatmap.external_beatmap_id}/{hashlib.sha256(content).hexdigest()}.osu",
                len(content),
                "application/x-osu-beatmap",
                hashlib.sha256(content).digest(),
            ),
        )
        for beatmap, content in zip(beatmaps, contents, strict=True)
    )
    return beatmapset, files


def test_equal_version_snapshot_must_not_replace_or_remove_current_revisions() -> None:
    current_snapshot = snapshot()
    current_metadata = _snapshot_beatmap_metadata(current_snapshot.beatmaps[0])
    conflicting = replace(
        current_snapshot,
        beatmaps=(replace(current_snapshot.beatmaps[0], md5=b"x" * 16),),
    )
    extended = replace(
        current_snapshot,
        beatmaps=(
            current_snapshot.beatmaps[0],
            replace(current_snapshot.beatmaps[0], external_beatmap_id=102, md5=b"z" * 16),
        ),
    )

    assert _snapshot_extends_current_revision_set({}, current_snapshot)
    assert _snapshot_extends_current_revision_set({100: current_metadata}, current_snapshot)
    assert not _snapshot_extends_current_revision_set({100: current_metadata}, conflicting)
    assert not _snapshot_extends_current_revision_set({100: None}, current_snapshot)
    assert _snapshot_extends_current_revision_set(
        {100: current_metadata, 101: None},
        extended,
    )
    assert not _snapshot_extends_current_revision_set(
        {100: current_metadata, 101: current_metadata},
        current_snapshot,
    )
    assert _snapshot_beatmapset_metadata(current_snapshot) != _snapshot_beatmapset_metadata(
        replace(current_snapshot, status="loved")
    )


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_content_repository_sync_query_count_is_bounded_and_history_is_resurrected(
    postgres_database_url: str,
) -> None:
    del postgres_database_url
    engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(engine)
    statement_count = 0
    counting = False

    def count_statement(*args: object) -> None:
        del args
        nonlocal statement_count
        if counting:
            statement_count += 1

    event.listen(engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        single_snapshot, single_files = snapshot_files((b"single",), beatmapset_id=201)
        original_contents = tuple(f"bulk-{index}".encode() for index in range(32))
        bulk_snapshot, bulk_files = snapshot_files(original_contents, beatmapset_id=202)
        async with session_factory.begin() as session:
            repository = SqlAlchemyContentRepository(session)

            counting = True
            single_result = await repository.synchronize_beatmapset(
                single_snapshot,
                single_files,
                now=NOW,
                next_check_at=NOW + timedelta(hours=8),
            )
            counting = False
            single_query_count = statement_count
            statement_count = 0

            counting = True
            bulk_result = await repository.synchronize_beatmapset(
                bulk_snapshot,
                bulk_files,
                now=NOW,
                next_check_at=NOW + timedelta(hours=8),
            )
            counting = False
            bulk_query_count = statement_count

            assert single_result.created_revision_count == 1
            assert bulk_result.created_revision_count == 32
            assert single_query_count == bulk_query_count
            assert bulk_query_count <= 13

            replacement_contents = (b"bulk-replacement", *original_contents[1:-1])
            replacement_snapshot, replacement_files = snapshot_files(
                replacement_contents,
                beatmapset_id=202,
                updated_at=NOW + timedelta(seconds=1),
            )
            replacement_snapshot = replace(
                replacement_snapshot,
                beatmaps=(
                    replace(replacement_snapshot.beatmaps[0], status="loved"),
                    *replacement_snapshot.beatmaps[1:],
                ),
            )
            replacement_files = tuple(
                replace(file, beatmap=beatmap)
                for file, beatmap in zip(replacement_files, replacement_snapshot.beatmaps, strict=True)
            )
            replacement_result = await repository.synchronize_beatmapset(
                replacement_snapshot,
                replacement_files,
                now=NOW + timedelta(seconds=1),
                next_check_at=NOW + timedelta(hours=8),
            )
            assert replacement_result.created_revision_count == 1
            assert replacement_result.unchanged_revision_count == 30
            assert replacement_result.removed_beatmap_count == 1

            restored_contents = original_contents[:-1]
            restored_snapshot, restored_files = snapshot_files(
                restored_contents,
                beatmapset_id=202,
                updated_at=NOW + timedelta(seconds=2),
            )
            restored_snapshot = replace(
                restored_snapshot,
                beatmaps=(replace(restored_snapshot.beatmaps[0], status="loved"), *restored_snapshot.beatmaps[1:]),
            )
            restored_files = tuple(
                replace(file, beatmap=beatmap)
                for file, beatmap in zip(restored_files, restored_snapshot.beatmaps, strict=True)
            )
            restored_result = await repository.synchronize_beatmapset(
                restored_snapshot,
                restored_files,
                now=NOW + timedelta(seconds=2),
                next_check_at=NOW + timedelta(hours=8),
            )
            assert restored_result.created_revision_count == 1
            assert restored_result.unchanged_revision_count == 30

            first_beatmap_id = await session.scalar(
                select(Beatmap.id).where(Beatmap.external_id == restored_snapshot.beatmaps[0].external_beatmap_id)
            )
            assert first_beatmap_id is not None
            revisions = tuple(
                await session.scalars(
                    select(BeatmapRevision)
                    .where(BeatmapRevision.beatmap_id == first_beatmap_id)
                    .order_by(BeatmapRevision.id)
                )
            )
            assert len(revisions) == 2
            assert sum(revision.is_current for revision in revisions) == 1
            assert (
                next(revision for revision in revisions if revision.is_current).sha256
                == hashlib.sha256(original_contents[0]).digest()
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(BeatmapStatusEvent)
                    .where(BeatmapStatusEvent.beatmap_id == first_beatmap_id)
                )
                == 1
            )

            stale_result = await repository.synchronize_beatmapset(
                bulk_snapshot,
                bulk_files,
                now=NOW + timedelta(seconds=3),
                next_check_at=NOW + timedelta(hours=8),
            )
            assert not stale_result.published
            assert stale_result.created_revision_count == 0
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", count_statement)
        await engine.dispose()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_content_repository_rating_uses_one_read_and_two_write_queries(postgres_database_url: str) -> None:
    del postgres_database_url
    engine = await infra_db.create_engine()
    session_factory = infra_db.create_session_factory(engine)
    statement_count = 0
    counting = False

    def count_statement(*args: object) -> None:
        del args
        nonlocal statement_count
        if counting:
            statement_count += 1

    event.listen(engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        beatmapset, files = snapshot_files((b"rating",), beatmapset_id=203)
        async with session_factory.begin() as session:
            repository = SqlAlchemyContentRepository(session)
            synchronized = await repository.synchronize_beatmapset(
                beatmapset,
                files,
                now=NOW,
                next_check_at=NOW + timedelta(hours=8),
            )
            beatmap_id = await session.scalar(
                select(Beatmap.id).where(Beatmap.beatmapset_id == synchronized.beatmapset_id)
            )
            assert beatmap_id is not None

            counting = True
            empty = await repository.get_rating(beatmap_id, 1)
            counting = False
            assert statement_count == 1
            assert empty.average is None and empty.vote_count == 0 and empty.account_rating is None

            statement_count = 0
            counting = True
            public = await repository.get_rating(beatmap_id, None)
            counting = False
            assert statement_count == 1
            assert public == empty

            statement_count = 0
            counting = True
            rated = await repository.rate(1, beatmap_id, 8)
            counting = False
            assert statement_count == 2
            assert rated.average == Decimal(8)
            assert rated.vote_count == 1
            assert rated.account_rating == 8

            statement_count = 0
            counting = True
            unknown = await repository.rate(1, beatmap_id + 100_000, 10)
            counting = False
            assert statement_count == 2
            assert unknown.average is None and unknown.vote_count == 0 and unknown.account_rating is None
            assert await session.scalar(select(func.count()).select_from(RatingVote)) == 1
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", count_statement)
        await engine.dispose()


def sync_service(
    source: FakeSource,
    calls: list[str],
) -> tuple[ContentSyncService, FakeRepository, FakeStorage, FakeOutbox, list[FakeUnitOfWork]]:
    repository = FakeRepository(calls)
    storage = FakeStorage(calls)
    outbox = FakeOutbox(calls)
    units: list[FakeUnitOfWork] = []

    def uow_factory() -> FakeUnitOfWork:
        unit = FakeUnitOfWork(calls)
        units.append(unit)
        return unit

    service = ContentSyncService(
        uow_factory=uow_factory,
        repository_factory=lambda session: cast(ContentRepository, repository),
        outbox_writer_factory=lambda session: outbox,
        upstream=source,
        object_storage=cast(ObjectStorage, storage),
        clock=cast(Clock, FixedClock()),
        id_generator=cast(IdGenerator, FakeIds()),
    )
    return service, repository, storage, outbox, units


@pytest.mark.asyncio
async def test_content_sync_verifies_and_stores_files_before_short_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    source = FakeSource(snapshot(), BEATMAP_FILE)
    service, repository, storage, outbox, units = sync_service(source, calls)
    logged: list[tuple[str, str, dict[str, object]]] = []
    monkeypatch.setattr(
        "perfcho.modules.content.services.log_event",
        lambda level, event, **fields: logged.append((level, event, fields)),
    )

    result = await service.synchronize(200)

    assert result == ContentSyncResult(20, 200, 1, 0, 0)
    assert calls == ["object-put", "transaction-enter", "repository", "outbox", "commit", "transaction-exit"]
    assert units[0].committed
    assert storage.keys[0].startswith("beatmaps/osu/200/100/")
    assert repository.files[0].stored_object.sha256 == hashlib.sha256(BEATMAP_FILE).digest()
    assert outbox.events[0].event_type == "content.beatmapset-synchronized.v1"
    assert [(level, event) for level, event, _ in logged] == [
        ("INFO", "content.sync.started"),
        ("INFO", "content.sync.committed"),
    ]
    assert logged[1][2]["beatmapset_id"] == 20
    assert not {"creator_name", "artist", "title", "file_name"} & logged[1][2].keys()


@pytest.mark.asyncio
async def test_content_sync_resolves_unknown_revision_after_blocking_fill() -> None:
    calls: list[str] = []
    source = FakeSource(snapshot(), BEATMAP_FILE)
    service, repository, _, _, _ = sync_service(source, calls)
    checksum = hashlib.md5(BEATMAP_FILE, usedforsecurity=False).hexdigest()

    resolved = await service.resolve_revision(
        checksum,
        "Artist - Title (Creator) [Insane].osu",
        200,
    )

    assert resolved.md5_hex == checksum
    assert resolved.is_current
    assert repository.snapshot is not None
    assert source.calls == [100]


@pytest.mark.asyncio
async def test_content_sync_does_not_emit_event_for_rejected_stale_snapshot() -> None:
    calls: list[str] = []
    source = FakeSource(snapshot(), BEATMAP_FILE)
    service, repository, _, outbox, _ = sync_service(source, calls)
    repository.published = False

    result = await service.synchronize(200)

    assert not result.published
    assert outbox.events == []
    assert calls == ["object-put", "transaction-enter", "repository", "commit", "transaction-exit"]


@pytest.mark.asyncio
async def test_content_refresh_skips_upstream_when_watermark_is_not_due() -> None:
    calls: list[str] = []
    source = FakeSource(snapshot(), BEATMAP_FILE)
    service, repository, storage, _, _ = sync_service(source, calls)

    await service.refresh_if_due(200)

    assert repository.refresh_claims == [200]
    assert source.calls == []
    assert storage.keys == []


@pytest.mark.asyncio
async def test_content_refresh_failure_is_fenced_to_claimed_lease() -> None:
    calls: list[str] = []
    source = FakeSource(snapshot(), b"different")
    service, repository, _, _, _ = sync_service(source, calls)
    repository.refresh_claimed = True

    await service.refresh_if_due(200)

    assert repository.refresh_failures == [
        (
            NOW + timedelta(minutes=30),
            NOW,
            NOW + timedelta(minutes=5),
            "upstream_content_unavailable",
        )
    ]


@pytest.mark.asyncio
async def test_content_sync_shutdown_cancels_and_drains_inflight_work() -> None:
    class BlockingSource(FakeSource):
        def __init__(self) -> None:
            super().__init__(snapshot(), BEATMAP_FILE)
            self.started = asyncio.Event()
            self.cancelled = False

        async def fetch_beatmapset(self, external_beatmapset_id: int) -> UpstreamBeatmapsetSnapshot:
            del external_beatmapset_id
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            raise AssertionError("unreachable")

    calls: list[str] = []
    source = BlockingSource()
    service, _, _, _, _ = sync_service(source, calls)
    resolution = asyncio.create_task(
        service.resolve_revision(
            hashlib.md5(BEATMAP_FILE, usedforsecurity=False).hexdigest(),
            "Artist - Title (Creator) [Insane].osu",
            200,
        )
    )
    await source.started.wait()

    await service.aclose()
    result = (await asyncio.gather(resolution, return_exceptions=True))[0]

    assert source.cancelled
    assert isinstance(result, asyncio.CancelledError)


@pytest.mark.asyncio
async def test_content_sync_rejects_mismatched_file_before_storage_or_transaction() -> None:
    calls: list[str] = []
    source = FakeSource(snapshot(), b"different")
    service, _, storage, outbox, units = sync_service(source, calls)

    with pytest.raises(UpstreamContentUnavailable, match="MD5"):
        await service.synchronize(200)

    assert storage.keys == []
    assert outbox.events == []
    assert units == []


@pytest.mark.asyncio
async def test_osu_upstream_source_normalizes_metadata_and_bounds_file_body() -> None:
    checksum = hashlib.md5(BEATMAP_FILE, usedforsecurity=False).hexdigest()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/oauth/token":
            return httpx.Response(200, json={"access_token": "token", "expires_in": 3600})
        if request.url.path == "/api/v2/beatmapsets/200":
            return httpx.Response(
                200,
                headers={"ETag": "set-etag"},
                json={
                    "id": 200,
                    "user_id": 12,
                    "creator": "Creator",
                    "artist": "Artist|Name",
                    "artist_unicode": "Artist",
                    "title": "Title",
                    "title_unicode": "Title",
                    "source": "Source",
                    "tags": "tag",
                    "genre": {"id": 2},
                    "language": {"id": 3},
                    "description": {"description": "Description"},
                    "status": "ranked",
                    "submitted_date": "2026-07-20T12:30:00Z",
                    "ranked_date": "2026-07-21T12:30:00Z",
                    "last_updated": "2026-07-29T12:30:00Z",
                    "availability": {"download_disabled": False},
                    "nsfw": False,
                    "video": True,
                    "beatmaps": [
                        {
                            "id": 100,
                            "beatmapset_id": 200,
                            "checksum": checksum,
                            "version": "Insane|Diff",
                            "mode": "osu",
                            "status": "ranked",
                            "last_updated": "2026-07-29T12:30:00Z",
                            "total_length": 120,
                            "hit_length": 100,
                            "bpm": 180.0,
                            "cs": 4.0,
                            "accuracy": 8.0,
                            "ar": 9.0,
                            "drain": 6.0,
                            "count_circles": 300,
                            "count_sliders": 190,
                            "count_spinners": 10,
                            "max_combo": 750,
                            "difficulty_rating": 5.25,
                        }
                    ],
                },
            )
        if request.url.path == "/osu/100":
            return httpx.Response(200, content=BEATMAP_FILE)
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    source = BanchoUpstreamContentSource(
        api_base_url="https://osu.test/api/v2",
        token_url="https://osu.test/oauth/token",
        client_id=1,
        client_secret="secret",
        beatmap_file_base_url="https://osu.test/osu",
        max_beatmap_file_bytes=1024,
        client=client,
    )
    try:
        result = await source.fetch_beatmapset(200)
        file_content = await source.fetch_beatmap_file(100)
    finally:
        await client.aclose()

    assert result.etag == "set-etag"
    assert result.artist == "Artist|Name"
    assert result.beatmaps[0].object_count == 500
    assert result.beatmaps[0].file_name == "Artist_Name - Title (Creator) [Insane_Diff].osu"
    assert file_content == BEATMAP_FILE
    assert requests[1].headers["authorization"] == "Bearer token"


@pytest.mark.asyncio
async def test_osu_upstream_lookup_falls_back_from_checksum_to_filename() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/oauth/token":
            return httpx.Response(200, json={"access_token": "token", "expires_in": 3600})
        if request.url.path == "/api/v2/beatmaps/lookup":
            if "checksum" in request.url.params:
                return httpx.Response(404)
            return httpx.Response(200, json={"id": 100, "beatmapset_id": 200})
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    source = BanchoUpstreamContentSource(
        api_base_url="https://osu.test/api/v2",
        token_url="https://osu.test/oauth/token",
        client_id=1,
        client_secret="secret",
        beatmap_file_base_url="https://osu.test/osu",
        max_beatmap_file_bytes=1024,
        client=client,
    )
    try:
        beatmapset_id = await source.lookup_beatmapset_id("a" * 32, "Artist - Title (Creator) [Diff].osu")
    finally:
        await client.aclose()

    assert beatmapset_id == 200
    assert [request.url.params for request in requests[1:]] == [
        httpx.QueryParams({"checksum": "a" * 32}),
        httpx.QueryParams({"filename": "Artist - Title (Creator) [Diff].osu"}),
    ]
