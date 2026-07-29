import hashlib
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from typing import cast

import httpx
import pytest

from perfcho.infra.upstream.osu import OsuUpstreamContentSource
from perfcho.modules.common import Clock, IdGenerator, ObjectStorage, PendingEvent, StoredObject
from perfcho.modules.content import (
    ContentInputRejected,
    ContentSyncResult,
    ContentSyncService,
    SyncedBeatmapFile,
    UpstreamBeatmapsetSnapshot,
    UpstreamBeatmapSnapshot,
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

    async def synchronize_beatmapset(
        self,
        snapshot: UpstreamBeatmapsetSnapshot,
        files: tuple[SyncedBeatmapFile, ...],
        *,
        now: datetime,
    ) -> ContentSyncResult:
        assert now == NOW
        self.calls.append("repository")
        self.snapshot = snapshot
        self.files = files
        return ContentSyncResult(20, snapshot.external_beatmapset_id, len(files), 0, 0)


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
async def test_content_sync_verifies_and_stores_files_before_short_transaction() -> None:
    calls: list[str] = []
    source = FakeSource(snapshot(), BEATMAP_FILE)
    service, repository, storage, outbox, units = sync_service(source, calls)

    result = await service.synchronize(200)

    assert result == ContentSyncResult(20, 200, 1, 0, 0)
    assert calls == ["object-put", "transaction-enter", "repository", "outbox", "commit", "transaction-exit"]
    assert units[0].committed
    assert storage.keys[0].startswith("beatmaps/osu/200/100/")
    assert repository.files[0].stored_object.sha256 == hashlib.sha256(BEATMAP_FILE).digest()
    assert outbox.events[0].event_type == "content.beatmapset-synchronized.v1"


@pytest.mark.asyncio
async def test_content_sync_rejects_mismatched_file_before_storage_or_transaction() -> None:
    calls: list[str] = []
    source = FakeSource(snapshot(), b"different")
    service, _, storage, outbox, units = sync_service(source, calls)

    with pytest.raises(ContentInputRejected, match="MD5"):
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
    source = OsuUpstreamContentSource(
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
