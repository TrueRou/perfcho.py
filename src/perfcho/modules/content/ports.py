"""Define transaction-bound content persistence ports."""

from datetime import datetime
from typing import Protocol

from perfcho.modules.common.ports import OutboxWriter, UnitOfWork
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


class ContentUnitOfWork(UnitOfWork, Protocol):
    """Expose the transaction resource used by content adapters."""

    @property
    def session(self) -> object:
        """Return the active transaction resource."""
        ...


class ContentRepository(Protocol):
    """Query and mutate content without exposing persistence entities."""

    async def lookup_md5(self, md5: bytes) -> BeatmapRevisionView | None:
        """Resolve one immutable revision by MD5."""
        ...

    async def lookup_beatmap(self, beatmap_id: int, *, external: bool) -> BeatmapRevisionView | None:
        """Resolve a current revision by canonical or external ID."""
        ...

    async def lookup_filename(self, file_name_key: str) -> BeatmapRevisionView | None:
        """Resolve a current revision by normalized filename."""
        ...

    async def batch_lookup(
        self,
        file_name_keys: tuple[str, ...],
        external_beatmap_ids: tuple[int, ...],
    ) -> tuple[BeatmapRevisionView, ...]:
        """Resolve a bounded mixed selector batch."""
        ...

    async def get_beatmapset(self, beatmapset_id: int, *, external: bool) -> BeatmapsetView | None:
        """Return a set and all current revisions."""
        ...

    async def search(self, query: ContentSearch) -> ContentSearchPage:
        """Search locally indexed beatmapsets."""
        ...

    async def list_favourites(self, account_id: int) -> tuple[int, ...]:
        """List external IDs favourited by an account."""
        ...

    async def set_favourite(self, account_id: int, beatmapset_id: int, favourited: bool) -> FavouriteResult:
        """Set the account favourite state."""
        ...

    async def get_rating(self, beatmapset_id: int, account_id: int | None) -> RatingSummary:
        """Return aggregate and optional account rating."""
        ...

    async def rate(self, account_id: int, beatmapset_id: int, rating: int) -> RatingSummary:
        """Upsert an account rating."""
        ...

    async def synchronize_beatmapset(
        self,
        snapshot: UpstreamBeatmapsetSnapshot,
        files: tuple[SyncedBeatmapFile, ...],
        *,
        now: datetime,
    ) -> ContentSyncResult:
        """Persist one complete immutable upstream snapshot."""
        ...


class UpstreamContentSource(Protocol):
    """Fetch authoritative beatmap metadata and bounded revision files."""

    async def fetch_beatmapset(self, external_beatmapset_id: int) -> UpstreamBeatmapsetSnapshot:
        """Fetch one complete upstream beatmapset snapshot."""
        ...

    async def fetch_beatmap_file(self, external_beatmap_id: int) -> bytes:
        """Fetch one bounded current .osu revision body."""
        ...


class ContentRepositoryFactory(Protocol):
    """Bind content persistence to a caller-owned transaction."""

    def __call__(self, session: object) -> ContentRepository:
        """Return a transaction-bound content repository."""
        ...


class ContentOutboxWriterFactory(Protocol):
    """Bind an outbox writer to the content synchronization transaction."""

    def __call__(self, session: object) -> OutboxWriter:
        """Return a transaction-bound outbox writer."""
        ...
